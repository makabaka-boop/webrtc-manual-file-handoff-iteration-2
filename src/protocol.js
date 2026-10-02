// 线上协议常量与编解码。
//
// 控制消息走 DataChannel 的文本通道（JSON），数据块走二进制通道：
//   { kind: 'meta',        id, name, size, chunks, hash }  首次传输开始
//   { kind: 'resume',      id, name, size, chunks, hash,
//                          nextChunk, prefixHash }          接收方请求恢复
//   { kind: 'resume-ack',  id, ok, nextChunk?, prefixHash?, reason? }
//                                                           发送方核对结果
//   { kind: 'end',         id, hash }                       传输结束（hash 供再次校验）
//   { kind: 'receipt',     id, ok, hash?, reason? }         接收方校验回执
//   { kind: 'cancel',      id }                             取消传输
//   ArrayBuffer: 前 4 字节为大端序号，其后为块载荷（<= CHUNK_SIZE 字节）
//
// 首次传输与恢复尝试共用同一个逻辑传输 id；id/name/size/chunks/hash 构成传输身份。
// 每次连接/恢复还另有本地尝试代际，旧通道的迟到事件不能凭相同 id 完成新尝试。
// 发送方只有收到 ok:true 且 hash 匹配的回执后才能把本次传输标记为完成。

export const CHUNK_SIZE = 16 * 1024; // 至多 16 KiB
export const MAX_FILE_SIZE = 5 * 1024 * 1024; // 不超过 5 MiB

const CHUNK_HEADER_BYTES = 4;

/** 生成一次传输的唯一 id（随机，避免重连后旧 id 撞上新 id）。 */
export function createTransferId() {
  // 优先使用 Web Crypto；浏览器与 Node >= 19 均提供 globalThis.crypto。
  const cryptoRef = globalThis.crypto;
  if (cryptoRef?.getRandomValues) {
    const buf = new Uint8Array(16);
    cryptoRef.getRandomValues(buf);
    // 加时间前缀，进一步降低复用页面时碰撞的概率。
    return `${Date.now().toString(36)}-${[...buf].map((b) => b.toString(16).padStart(2, '0')).join('')}`;
  }
  return `${Date.now().toString(36)}-${Math.random().toString(36).slice(2)}`;
}

/** 计算 ArrayBuffer / TypedArray / Blob 的 SHA-256（十六进制小写）。 */
export async function sha256(data) {
  let bytes;
  if (data instanceof ArrayBuffer) {
    bytes = data;
  } else if (ArrayBuffer.isView(data)) {
    bytes = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
  } else if (data instanceof Blob) {
    bytes = await data.arrayBuffer();
  } else {
    throw new TypeError('sha256: 不支持的数据类型');
  }
  const digest = await globalThis.crypto.subtle.digest('SHA-256', bytes);
  return [...new Uint8Array(digest)].map((b) => b.toString(16).padStart(2, '0')).join('');
}

export function encodeMeta({ id, name, size, chunks, hash }) {
  return JSON.stringify({ kind: 'meta', id, name, size, chunks, hash });
}

export function encodeResume({ id, name, size, chunks, hash, nextChunk, prefixHash }) {
  return JSON.stringify({
    kind: 'resume', id, name, size, chunks, hash, nextChunk, prefixHash,
  });
}

export function encodeResumeAck({ id, ok, nextChunk = 0, prefixHash = '', reason = '' }) {
  return JSON.stringify({
    kind: 'resume-ack', id, ok: Boolean(ok), nextChunk, prefixHash, reason,
  });
}

export function encodeEnd({ id, hash }) {
  return JSON.stringify({ kind: 'end', id, hash });
}

export function encodeReceipt({ id, ok, hash = '', reason = '' }) {
  return JSON.stringify({ kind: 'receipt', id, ok: Boolean(ok), hash, reason });
}

function readTransferIdentity(msg) {
  const { id, name, size, chunks, hash } = msg;
  if (typeof name !== 'string' || !Number.isInteger(size) || size < 0 || size > MAX_FILE_SIZE) return null;
  if (!Number.isInteger(chunks) || chunks < 0) return null;
  if (!/^[0-9a-f]{64}$/.test(typeof hash === 'string' ? hash : '')) return null;
  const expectedChunks = Math.ceil(size / CHUNK_SIZE);
  if (chunks !== expectedChunks) return null;
  return { id, name, size, chunks, hash };
}

/** 解析文本控制消息；非法消息返回 null，由调用方按坏消息处理。 */
export function parseControl(text) {
  let msg;
  try {
    msg = JSON.parse(text);
  } catch {
    return null;
  }
  if (!msg || typeof msg !== 'object' || typeof msg.kind !== 'string' || typeof msg.id !== 'string') {
    return null;
  }
  if (msg.kind === 'meta') {
    const identity = readTransferIdentity(msg);
    if (!identity) return null;
    return { kind: 'meta', ...identity };
  }
  if (msg.kind === 'resume') {
    const identity = readTransferIdentity(msg);
    if (!identity) return null;
    if (!Number.isInteger(msg.nextChunk) || msg.nextChunk < 0 || msg.nextChunk > identity.chunks) return null;
    if (typeof msg.prefixHash !== 'string' || msg.prefixHash.length === 0) return null;
    // 0 个完整块没有前缀，其摘要固定为空文件 SHA-256，仍必须显式携带以便双方共用身份描述。
    if (msg.nextChunk === 0 && !/^[0-9a-f]{64}$/.test(msg.prefixHash)) return null;
    if (msg.nextChunk > 0 && !/^[0-9a-f]{64}$/.test(msg.prefixHash)) return null;
    return { kind: 'resume', ...identity, nextChunk: msg.nextChunk, prefixHash: msg.prefixHash };
  }
  if (msg.kind === 'resume-ack') {
    if (typeof msg.ok !== 'boolean') return null;
    if (msg.ok) {
      if (!Number.isInteger(msg.nextChunk) || msg.nextChunk < 0) return null;
      if (typeof msg.prefixHash !== 'string' || msg.prefixHash.length === 0) return null;
      return {
        kind: 'resume-ack', id: msg.id, ok: true,
        nextChunk: msg.nextChunk, prefixHash: msg.prefixHash, reason: '',
      };
    }
    if (typeof msg.reason !== 'string' || msg.reason.length === 0) return null;
    return {
      kind: 'resume-ack', id: msg.id, ok: false,
      nextChunk: 0, prefixHash: '', reason: msg.reason.slice(0, 200),
    };
  }
  if (msg.kind === 'end') {
    if (!/^[0-9a-f]{64}$/.test(typeof msg.hash === 'string' ? msg.hash : '')) return null;
    return { kind: 'end', id: msg.id, hash: msg.hash };
  }
  if (msg.kind === 'receipt') {
    if (typeof msg.ok !== 'boolean') return null;
    if (msg.ok) {
      if (!/^[0-9a-f]{64}$/.test(typeof msg.hash === 'string' ? msg.hash : '')) return null;
      return { kind: 'receipt', id: msg.id, ok: true, hash: msg.hash, reason: '' };
    }
    if (typeof msg.reason !== 'string' || msg.reason.length === 0) return null;
    return { kind: 'receipt', id: msg.id, ok: false, hash: '', reason: msg.reason.slice(0, 200) };
  }
  if (msg.kind === 'cancel') {
    return { kind: 'cancel', id: msg.id };
  }
  return null;
}

/** 比较协议中所有决定“同一文件”的字段（id、文件名、总长、块数、整文件哈希）。 */
export function sameTransferIdentity(a, b) {
  return Boolean(a && b) &&
    a.id === b.id &&
    a.name === b.name &&
    a.size === b.size &&
    a.chunks === b.chunks &&
    a.hash === b.hash;
}

/** 计算接收方已暂存的连续前缀 SHA-256；存在空洞时抛错，绝不把缺口后的块算入前缀。 */
export async function digestChunkPrefix(chunks, nextChunk) {
  if (!Number.isInteger(nextChunk) || nextChunk < 0 || nextChunk > chunks.length) {
    throw new RangeError('无效的前缀边界');
  }
  const prefix = [];
  for (let i = 0; i < nextChunk; i += 1) {
    const chunk = chunks[i];
    if (!(chunk instanceof Uint8Array)) throw new Error(`前缀在块 #${i} 处不连续`);
    prefix.push(chunk);
  }
  return sha256(new Blob(prefix.length ? prefix : [new Uint8Array(0)]));
}

/** 把编号块打包为可直接发送的 ArrayBuffer（4B 大端序号 + 载荷）。 */
export function packChunk(sequence, payload) {
  const bytes = payload instanceof Uint8Array ? payload : new Uint8Array(payload);
  if (bytes.byteLength > CHUNK_SIZE) throw new RangeError(`块超过 ${CHUNK_SIZE} 字节`);
  const out = new ArrayBuffer(CHUNK_HEADER_BYTES + bytes.byteLength);
  new DataView(out).setUint32(0, sequence, false);
  new Uint8Array(out, CHUNK_HEADER_BYTES).set(bytes);
  return out;
}

/**
 * 解析二进制块。返回 { sequence, payload: Uint8Array }；
 * 头部不足或载荷为空时返回 null（坏块）。
 */
export function unpackChunk(buffer) {
  if (!(buffer instanceof ArrayBuffer) || buffer.byteLength < CHUNK_HEADER_BYTES + 1) return null;
  const sequence = new DataView(buffer, 0, CHUNK_HEADER_BYTES).getUint32(0, false);
  return { sequence, payload: new Uint8Array(buffer, CHUNK_HEADER_BYTES) };
}
