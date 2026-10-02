// 线上协议常量与编解码。
//
// 控制消息走 DataChannel 的文本通道（JSON），数据块走二进制通道：
//   { kind: 'meta',           id, name, size, chunks, hash }             传输开始
//   { kind: 'resume',         id, name, size, chunks, hash,
//                              nextChunk, prefixHash }                   接收方请求续传
//   { kind: 'resume-accept',  id, nextChunk, prefixHash }                发送方核对通过
//   { kind: 'resume-reject',  id, reason }                               续传身份/前缀不符
//   { kind: 'end',            id, hash }                                 传输结束
//   { kind: 'receipt',        id, ok, hash?, reason? }                   接收方校验回执
//   { kind: 'cancel',         id }                                       取消传输
//   ArrayBuffer: 前 4 字节为大端序号，其后为块载荷（<= CHUNK_SIZE 字节）
//
// 首次传输与每次续传尝试共用同一个逻辑传输 id；续传另外显式携带下一块序号和
// 此前连续前缀的 SHA-256。数据只保存在接收方当前页面内存中，不写入服务器。
//
// 每条控制消息都带本次传输 id；序号从 0 起，逐块递增。
// 发送方只有收到 ok:true 且 hash 匹配的回执后才能把本次传输标记为完成。

export const CHUNK_SIZE = 16 * 1024; // 至多 16 KiB
export const MAX_FILE_SIZE = 5 * 1024 * 1024; // 不超过 5 MiB

const CHUNK_HEADER_BYTES = 4;

export function chunksForSize(size) {
  return Math.ceil(size / CHUNK_SIZE);
}

/** 第 sequence 块之前已经形成的连续前缀字节数。 */
export function prefixBytesForSequence(size, sequence) {
  return Math.min(size, sequence * CHUNK_SIZE);
}

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

export function encodeResumeAccept({ id, nextChunk, prefixHash }) {
  return JSON.stringify({ kind: 'resume-accept', id, nextChunk, prefixHash });
}

export function encodeResumeReject({ id, reason }) {
  return JSON.stringify({ kind: 'resume-reject', id, reason });
}

export function encodeEnd({ id, hash }) {
  return JSON.stringify({ kind: 'end', id, hash });
}

export function encodeReceipt({ id, ok, hash = '', reason = '' }) {
  return JSON.stringify({ kind: 'receipt', id, ok: Boolean(ok), hash, reason });
}

export function encodeCancel({ id }) {
  return JSON.stringify({ kind: 'cancel', id });
}

/** 校验所有“原文件身份”字段：文件名、总长、块数和整文件 SHA-256。 */
function validateFileIdentity({ name, size, chunks, hash }) {
  if (typeof name !== 'string') return false;
  if (!Number.isInteger(size) || size < 0 || size > MAX_FILE_SIZE) return false;
  if (!Number.isInteger(chunks) || chunks !== chunksForSize(size)) return false;
  return typeof hash === 'string' && /^[0-9a-f]{64}$/.test(hash);
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
    const { id, name, size, chunks, hash } = msg;
    if (!validateFileIdentity({ name, size, chunks, hash })) return null;
    return { kind: 'meta', id, name, size, chunks, hash };
  }
  if (msg.kind === 'resume') {
    const { id, name, size, chunks, hash, nextChunk, prefixHash } = msg;
    if (!validateFileIdentity({ name, size, chunks, hash })) return null;
    if (!Number.isInteger(nextChunk) || nextChunk < 0 || nextChunk > chunks) return null;
    if (!/^[0-9a-f]{64}$/.test(prefixHash)) return null;
    return { kind: 'resume', id, name, size, chunks, hash, nextChunk, prefixHash };
  }
  if (msg.kind === 'resume-accept') {
    const { id, nextChunk, prefixHash } = msg;
    if (!Number.isInteger(nextChunk) || nextChunk < 0) return null;
    if (!/^[0-9a-f]{64}$/.test(prefixHash)) return null;
    return { kind: 'resume-accept', id: msg.id, nextChunk, prefixHash };
  }
  if (msg.kind === 'resume-reject') {
    if (typeof msg.reason !== 'string' || msg.reason.length === 0) return null;
    return { kind: 'resume-reject', id: msg.id, reason: msg.reason.slice(0, 200) };
  }
  if (msg.kind === 'end') {
    if (typeof msg.hash !== 'string') return null;
    return { kind: 'end', id: msg.id, hash: msg.hash };
  }
  if (msg.kind === 'receipt') {
    if (typeof msg.ok !== 'boolean') return null;
    if (msg.ok) {
      if (typeof msg.hash !== 'string' || msg.hash.length === 0) return null;
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
