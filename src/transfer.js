// 文件传输核心：FileSender 与 FileReceiver。
//
// 设计要点：
//  - 通道可替换：只依赖 RTCDataChannel 形状（send / bufferedAmount /
//    bufferedAmountLowThreshold / readyState / 事件 / close），便于注入测试替身。
//  - 背压：发送方按 bufferedAmount 高低水位暂停读取与发送，绝不一次塞满。
//  - 代际守卫：每次连接创建独立 attemptId，每次取消/中断/新校验还会提升本地
//    generation；旧通道、旧对象迟到的事件与异步结果一律作废。
//  - 显式续传：接收方只保存块边界完整的连续前缀，重连后发送 resume，声明原
//    传输 id、下一块序号、文件身份与前缀 SHA-256；发送方逐字节核对后才续发。
//  - 失败必须显式：任何一步不通都进入 'failed' / 'canceled'，不会伪造完成。

import {
  CHUNK_SIZE,
  MAX_FILE_SIZE,
  createTransferId,
  sha256,
  chunksForSize,
  prefixBytesForSequence,
  encodeMeta,
  encodeResume,
  encodeResumeAccept,
  encodeResumeReject,
  encodeEnd,
  encodeReceipt,
  encodeCancel,
  parseControl,
  packChunk,
  unpackChunk,
} from './protocol.js';

// 默认低水位（字节）。发送方会把 channel.bufferedAmountLowThreshold 设为此值；
// 一旦缓冲达到低水位的 HIGH_WATER_FACTOR 倍即暂停，等 bufferedamountlow 再继续。
const SEND_LOW_WATER = 64 * 1024;
const SEND_HIGH_WATER = 256 * 1024;
const HIGH_WATER_FACTOR = 4;
const DEFAULT_RECEIPT_TIMEOUT_MS = 30_000;

/** 当前通道的高水位：以配置的低水位阈值为准，缺省回落到常量。 */
function highWaterOf(channel) {
  const threshold = channel.bufferedAmountLowThreshold;
  if (typeof threshold === 'number' && threshold > 0) {
    return threshold * HIGH_WATER_FACTOR;
  }
  return SEND_HIGH_WATER;
}

const TERMINAL_STATES = new Set(['completed', 'canceled', 'failed']);
const RUNNING_STATES = ['hashing', 'active', 'confirming', 'resuming'];

/**
 * 可跨新连接携带的接收方续传身份。
 * 只存在于当前页面内存；chunks 是块边界完整且从 0 连续的前缀。
 */
export class ReceiverResume {
  constructor({
    id, name, size,
    totalChunks = undefined,
    chunks = undefined,
    savedChunks = undefined,
    hash, nextChunk, prefixHash,
  }) {
    this.id = id;
    this.name = name;
    this.size = size;
    this.totalChunks = totalChunks ?? (Array.isArray(chunks) ? undefined : chunks);
    this.hash = hash;
    this.nextChunk = nextChunk;
    this.prefixHash = prefixHash;
    // 交给新接收对象后转移所有权；快照本身在 session 中不再保留大块副本。
    this._chunks = savedChunks ?? (Array.isArray(chunks) ? chunks : undefined);
  }

  get prefixBytes() {
    return prefixBytesForSequence(this.size, this.nextChunk);
  }

  toRequest() {
    return {
      id: this.id,
      name: this.name,
      size: this.size,
      chunks: this.totalChunks,
      hash: this.hash,
      nextChunk: this.nextChunk,
      prefixHash: this.prefixHash,
    };
  }

  cloneWithoutChunks() {
    return new ReceiverResume({
      id: this.id,
      name: this.name,
      size: this.size,
      chunks: this.totalChunks,
      hash: this.hash,
      nextChunk: this.nextChunk,
      prefixHash: this.prefixHash,
    });
  }
}

export class FileSender extends EventTarget {
  /**
   * @param channel 已 open 的 DataChannel（或测试替身）
   * @param {object} [opts]
   * @param {string} [opts.attemptId] 本次连接尝试的独立代际；缺省自动生成
   * @param {ReceiverResume} [opts.resume] 接收方在新连接上带来的续传身份
   * @param {() => File|Blob|null} [opts.getResumeFile] 收到 resume 时取回当前所选文件
   * @param {number} [opts.lowThreshold] 缓冲低水位（字节），默认 64 KiB
   * @param {number} [opts.perChunkDelay] 每发一块后的等待毫秒数（仅测试用）
   * @param {number} [opts.receiptTimeoutMs] 等待回执超时，默认 30000；0 不超时
   */
  constructor(channel, {
    attemptId = createTransferId(),
    resume = null,
    lowThreshold = SEND_LOW_WATER,
    perChunkDelay = 0,
    receiptTimeoutMs = DEFAULT_RECEIPT_TIMEOUT_MS,
    getResumeFile = null,
  } = {}) {
    super();
    this._channel = channel;
    this.attemptId = attemptId;
    this._generation = 0;
    this._drainWaiters = [];
    this._receiptWaiters = [];
    this._receiptTimer = null;
    this._receiptTimeoutMs = receiptTimeoutMs;
    this._perChunkDelay = perChunkDelay;
    this._resumeIdentity = resume;
    this._getResumeFile = typeof getResumeFile === 'function' ? getResumeFile : null;
    this._resumeInFlight = false;
    this.state = 'idle';
    this.id = resume?.id ?? null;
    this.file = null;
    this._sentBytes = 0;
    this._startSequence = 0;
    this._startBytes = 0;
    channel.bufferedAmountLowThreshold = lowThreshold;
    channel.addEventListener('close', this._onChannelClose);
    channel.addEventListener('error', this._onChannelError);
    channel.addEventListener('message', this._onMessage);
  }

  /** 解除对通道的监听（换新连接或彻底放弃该发送器时调用）。 */
  destroy() {
    const ch = this._channel;
    ch.removeEventListener?.('close', this._onChannelClose);
    ch.removeEventListener?.('error', this._onChannelError);
    ch.removeEventListener?.('message', this._onMessage);
  }

  get sentBytes() {
    return this._sentBytes;
  }

  get nextChunk() {
    return this._startSequence;
  }

  get resumeIdentity() {
    return this._resumeIdentity;
  }

  _setState(state, detail = {}) {
    this.state = state;
    this.dispatchEvent(new CustomEvent('state', { detail: { state, ...detail } }));
  }

  _emitProgress() {
    const file = this.file;
    this.dispatchEvent(
      new CustomEvent('progress', {
        detail: { loaded: this._sentBytes, total: file ? file.size : 0 },
      }),
    );
  }

  /** 中断但保留页面上的文件选择；接收方可能在新连接请求续传。 */
  _interrupt(reason) {
    if (!RUNNING_STATES.includes(this.state)) return;
    this._generation += 1;
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._setState('interrupted', { reason, id: this.id });
  }

  /** 关闭旧连接前挂起发送器；不删除 File 对象（File 来自页面 input）。 */
  suspendForReconnect(reason = '重新连接') {
    this._interrupt(reason);
    this.destroy();
  }

  /**
   * 首次发送一个文件。
   * @returns 传输 id；校验失败或通道不可用时 throw（状态进入 failed）。
   */
  async start(file) {
    if (RUNNING_STATES.includes(this.state) || this.state === 'interrupted') {
      throw new Error('已有传输正在进行');
    }
    const channel = this._channel;
    if (channel.readyState !== 'open') {
      this._setState('failed', { reason: '通道未打开' });
      throw new Error('通道未打开，无法发送');
    }
    const reason = this._validateFile(file);
    if (reason) {
      this._setState('failed', { reason });
      throw new Error(reason);
    }

    const generation = ++this._generation;
    this._resumeIdentity = null;
    this.file = file;
    this.id = createTransferId();
    this._sentBytes = 0;
    this._startSequence = 0;
    this._startBytes = 0;
    this._setState('hashing');

    let hash;
    try {
      hash = await sha256(file);
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._setState('failed', { reason: `计算哈希失败：${err.message || err}` });
      }
      throw err;
    }
    if (generation !== this._generation || TERMINAL_STATES.has(this.state)) return this.id;

    this._setState('active');
    try {
      const chunks = chunksForSize(file.size);
      channel.send(encodeMeta({ id: this.id, name: file.name, size: file.size, chunks, hash }));
      await this._pump(generation, 0);
      await this._finishAfterPump(generation, hash);
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._setState('failed', { reason: err.message || String(err) });
      }
    }
    return this.id;
  }

  /** 响应接收方在新连接上显式发起的 resume。 */
  async resume(file) {
    const identity = this._resumeIdentity;
    if (!identity) throw new Error('没有可恢复的传输');
    if (this.state !== 'idle') throw new Error('当前状态不能恢复传输');
    const channel = this._channel;
    if (channel.readyState !== 'open') {
      this._rejectResume(identity, '新通道未打开', false);
      throw new Error('通道未打开，无法恢复');
    }

    const invalid = this._validateFile(file);
    if (invalid || file.name !== identity.name || file.size !== identity.size) {
      this._rejectResume(identity, '所选文件与原传输身份不一致');
      throw new Error(invalid || '所选文件与原传输身份不一致');
    }

    const generation = ++this._generation;
    this.file = file;
    this.id = identity.id;
    this._startSequence = identity.nextChunk;
    this._startBytes = identity.prefixBytes;
    this._sentBytes = this._startBytes;
    this._setState('resuming', { id: this.id });

    // 接收方已经发送 resume；这里先做本地核对，通过后只回 resume-accept。
    let hash;
    try {
      hash = await sha256(file);
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._rejectResume(identity, `计算哈希失败：${err.message || err}`);
      }
      throw err;
    }
    if (generation !== this._generation || this.state !== 'resuming') return;
    if (hash !== identity.hash) {
      this._rejectResume(identity, '整文件 SHA-256 与原传输身份不一致');
      return;
    }

    const prefixSize = identity.prefixBytes;
    let prefixBuffer = new ArrayBuffer(0);
    try {
      prefixBuffer = await file.slice(0, prefixSize).arrayBuffer();
      const prefixHash = await sha256(prefixBuffer);
      if (generation !== this._generation || this.state !== 'resuming') return;
      if (prefixHash !== identity.prefixHash) {
        this._rejectResume(identity, '已收前缀的 SHA-256 与所选文件不一致');
        return;
      }
    } catch (err) {
      if (generation === this._generation && this.state === 'resuming') {
        this._rejectResume(identity, `核对已收前缀失败：${err.message || err}`);
      }
      return;
    }

    try {
      channel.send(encodeResumeAccept({
        id: identity.id,
        nextChunk: identity.nextChunk,
        prefixHash: identity.prefixHash,
      }));
      this._setState('active', { id: identity.id, resumed: true });
      this._emitProgress();
      await this._pump(generation, identity.nextChunk);
      await this._finishAfterPump(generation, hash);
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._setState('failed', { reason: err.message || String(err) });
      }
    }
  }

  _validateFile(file) {
    if (!file || typeof file.size !== 'number') return '无效的文件';
    if (file.size > MAX_FILE_SIZE) return `文件超过 ${MAX_FILE_SIZE} 字节上限`;
    return '';
  }

  _rejectResume(identity, reason, notify = true) {
    if (this.state === 'failed' || this.state === 'canceled') return;
    if (notify && this._channel.readyState === 'open') {
      try {
        this._channel.send(encodeResumeReject({ id: identity.id, reason }));
      } catch {
        // 本地仍必须明确失败。
      }
    }
    this._generation += 1;
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._resumeIdentity = null;
    this._setState('failed', { reason, id: identity.id });
  }

  /** pump 正常返回后发 end，并等待带完整哈希的成功回执。 */
  async _finishAfterPump(generation, hash) {
    const channel = this._channel;
    if (generation !== this._generation || this.state !== 'active') return;
    channel.send(encodeEnd({ id: this.id, hash }));
    this._setState('confirming', { id: this.id, hash });
    await this._waitForReceipt(generation, hash);
  }

  /** 逐块读取文件并按背压写入通道。 */
  async _pump(generation, startSequence) {
    const channel = this._channel;
    const file = this.file;
    const highWater = highWaterOf(channel);
    let sequence = startSequence;
    let offset = prefixBytesForSequence(file.size, startSequence);
    while (offset < file.size) {
      if (generation !== this._generation) {
        throw new Error('传输已被取代（取消或重连）');
      }
      if (channel.readyState !== 'open') {
        throw new Error('通道已关闭，发送中断');
      }

      // 背压：通道积压达到高水位时，等 bufferedamountlow 事件；
      // 期间若关闭/出错/取消则立即醒来并失败，不继续塞数据。
      if (channel.bufferedAmount >= highWater) {
        await this._waitForDrain(generation);
      }

      const end = Math.min(offset + CHUNK_SIZE, file.size);
      const block = new Uint8Array(await file.slice(offset, end).arrayBuffer());
      if (generation !== this._generation) throw new Error('传输已取消');
      if (channel.readyState !== 'open') throw new Error('通道已关闭，发送中断');

      channel.send(packChunk(sequence, block));
      this._sentBytes += block.byteLength;
      this._emitProgress();
      offset = end;
      sequence += 1;

      // 每次 send 后立即检查：一旦越线且后面还有块才暂停。最后一块之后直接进入
      // end；end 是很小的控制消息，不能因错过 low 事件永久卡住。
      if (channel.bufferedAmount >= highWater && offset < file.size) {
        await this._waitForDrain(generation);
      } else if (this._perChunkDelay > 0) {
        // 测试钩子：在超快链路上人为放慢每块，制造可取消/断线窗口。
        await new Promise((resolve) => setTimeout(resolve, this._perChunkDelay));
        if (generation !== this._generation) throw new Error('传输已取消');
      }
    }
  }

  /** 等待发送缓冲回落到低水位；被关闭、出错或取消抢占时 reject。 */
  _waitForDrain(generation) {
    const channel = this._channel;
    return new Promise((resolve, reject) => {
      const waiter = { generation, resolve, reject };
      this._drainWaiters.push(waiter);
      const cleanup = () => {
        const idx = this._drainWaiters.indexOf(waiter);
        if (idx >= 0) this._drainWaiters.splice(idx, 1);
        channel.removeEventListener('bufferedamountlow', onLow);
        channel.removeEventListener('close', onClose);
        channel.removeEventListener('error', onError);
      };
      const onLow = () => {
        cleanup();
        resolve();
      };
      const onClose = () => {
        cleanup();
        reject(new Error('等待缓冲回落时通道关闭'));
      };
      const onError = () => {
        cleanup();
        reject(new Error('等待缓冲回落时通道出错'));
      };
      channel.addEventListener('bufferedamountlow', onLow);
      channel.addEventListener('close', onClose);
      channel.addEventListener('error', onError);
      if (generation !== this._generation) {
        cleanup();
        reject(new Error('传输已取消'));
      }
    });
  }

  /** 抢占所有正在等待缓冲回落的循环（取消/失败时调用）。 */
  _rejectDrainWaiters(reason) {
    const waiters = this._drainWaiters.splice(0);
    for (const w of waiters) w.reject(new Error(reason));
  }

  _clearReceiptTimer() {
    if (this._receiptTimer !== null) {
      clearTimeout(this._receiptTimer);
      this._receiptTimer = null;
    }
  }

  /** end 已写入通道后，等待接收方“完成全部校验”的明确回执。 */
  _waitForReceipt(generation, expectedHash) {
    return new Promise((resolve, reject) => {
      const waiter = { generation, expectedHash, resolve, reject };
      this._receiptWaiters.push(waiter);
      if (this._receiptTimeoutMs > 0) {
        this._receiptTimer = setTimeout(() => {
          this._rejectReceiptWaiters('等待接收方校验回执超时');
          this.fail('等待接收方校验回执超时，不能确认传输成功');
        }, this._receiptTimeoutMs);
      }
      if (generation !== this._generation || this.state !== 'confirming') {
        this._rejectReceiptWaiters('传输已取消');
      }
    });
  }

  _rejectReceiptWaiters(reason) {
    this._clearReceiptTimer();
    const waiters = this._receiptWaiters.splice(0);
    for (const w of waiters) w.reject(new Error(reason));
  }

  _resolveReceiptWaiters(detail) {
    this._clearReceiptTimer();
    const waiters = this._receiptWaiters.splice(0);
    for (const w of waiters) w.resolve(detail);
  }

  /** 主动取消：通知对端丢弃（尽力而为），本地丢弃半成品。 */
  cancel(reason = '用户取消') {
    if (!['hashing', 'active', 'confirming', 'resuming'].includes(this.state)) return;
    this._generation += 1;
    const id = this.id;
    this._rejectDrainWaiters('传输已取消');
    this._rejectReceiptWaiters('传输已取消');
    try {
      if (this._channel.readyState === 'open' && id) {
        this._channel.send(encodeCancel({ id }));
      }
    } catch {
      // 通道可能已关闭；取消仍以本地状态为准。
    }
    this._sentBytes = 0;
    this._resumeIdentity = null;
    this._setState('canceled', { reason, id });
  }

  /** 外部（连接层）通报致命错误。 */
  fail(reason) {
    if (TERMINAL_STATES.has(this.state)) return;
    this._generation += 1;
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._resumeIdentity = null;
    this._setState('failed', { reason });
  }

  _onChannelClose = () => {
    this._interrupt('DataChannel 已关闭');
  };

  _onChannelError = () => {
    this._interrupt('DataChannel 发生错误');
  };

  _onMessage = (event) => {
    if (typeof event.data !== 'string') return;
    const msg = parseControl(event.data);
    if (!msg) {
      this._failIfRunning('收到无法识别的控制消息');
      return;
    }

    if (msg.kind === 'receipt') this._handleReceipt(msg);
    if (msg.kind === 'resume') this._handleResumeRequest(msg);
    if (msg.kind === 'resume-reject' && this.state === 'resuming' && msg.id === this.id) {
      this.fail(`接收方拒绝恢复：${msg.reason}`);
      return;
    }
    // 只认当前传输的取消通知；旧传输迟到的 cancel 不能影响新传输。
    if (msg.kind === 'cancel' && msg.id === this.id && RUNNING_STATES.includes(this.state)) {
      this._generation += 1;
      this._rejectDrainWaiters('对端取消了传输');
      this._rejectReceiptWaiters('对端取消了传输');
      this._sentBytes = 0;
      this._resumeIdentity = null;
      this._setState('canceled', { reason: '接收方取消了传输', id: msg.id });
    }
  };

  _failIfRunning(reason) {
    if (RUNNING_STATES.includes(this.state)) this.fail(reason);
  }

  _handleResumeRequest(msg) {
    // 新连接的发送方必须是 idle；运行中/终态对象收到 resume 都是旧连接或协议错误。
    if (this.state !== 'idle' || this._resumeInFlight) return;
    const identity = new ReceiverResume({
      id: msg.id,
      name: msg.name,
      size: msg.size,
      totalChunks: msg.chunks,
      hash: msg.hash,
      nextChunk: msg.nextChunk,
      prefixHash: msg.prefixHash,
    });
    this._resumeIdentity = identity;
    const file = this._getResumeFile?.() ?? null;
    if (!file) {
      this._rejectResume(identity, '发送方尚未选择要恢复的文件');
      return;
    }
    this._resumeInFlight = true;
    this.resume(file).catch((err) => {
      if (this.state !== 'failed' && this.state !== 'canceled') {
        this._rejectResume(identity, err.message || String(err));
      }
    }).finally(() => {
      this._resumeInFlight = false;
    });
  }

  _handleReceipt(msg) {
    // 只处理当前传输的回执；旧 id 的迟到回执不能改变新传输。
    if (msg.id !== this.id) return;
    if (!['active', 'confirming'].includes(this.state)) return;

    if (!msg.ok) {
      this.fail(`接收方校验失败：${msg.reason}`);
      return;
    }
    if (this.state === 'active') {
      // 可靠有序通道上，成功回执不可能先于 end；收到即协议不一致。
      this.fail('接收方在 end 前发送了完成回执');
      return;
    }

    // 回执必须绑定完整文件哈希；只收到“成功”二字不算可核对的完成。
    const waiter = this._receiptWaiters.find((w) => w.generation === this._generation);
    if (!waiter || msg.hash !== waiter.expectedHash) {
      this.fail('接收方完成回执的哈希不一致');
      return;
    }

    this._resolveReceiptWaiters({ hash: msg.hash });
    this._resumeIdentity = null;
    this._setState('completed', { id: this.id, hash: msg.hash });
  }
}

export class FileReceiver extends EventTarget {
  /**
   * @param channel 已 open（或即将 open）的 DataChannel / 测试替身
   * @param {object} [opts]
   * @param {string} [opts.attemptId] 本次连接尝试的独立代际；缺省自动生成
   * @param {ReceiverResume} [opts.resume] 从上个连接保留下来的连续前缀身份
   */
  constructor(channel, { attemptId = createTransferId(), resume = null } = {}) {
    super();
    this._channel = channel;
    this.attemptId = attemptId;
    this._generation = 0;
    this.state = 'idle';
    this.id = null;
    this._chunks = [];
    this._meta = null;
    this._receivedBytes = 0;
    this._resume = null;
    this._suspended = false;
    if (resume) this._adoptResume(resume);
    channel.addEventListener('message', this._onMessage);
    channel.addEventListener('close', this._onChannelClose);
    channel.addEventListener('error', this._onChannelError);
  }

  destroy() {
    const ch = this._channel;
    ch.removeEventListener?.('message', this._onMessage);
    ch.removeEventListener?.('close', this._onChannelClose);
    ch.removeEventListener?.('error', this._onChannelError);
  }

  get receivedBytes() {
    return this._receivedBytes;
  }

  get nextChunk() {
    return this._chunks.length;
  }

  get info() {
    const meta = this._meta ?? this._resume;
    if (!meta) return null;
    return {
      id: this.id,
      name: meta.name,
      size: meta.size,
      chunks: meta.totalChunks ?? meta.chunks,
      hash: meta.hash,
    };
  }

  get resumeIdentity() {
    return this._resume;
  }

  _setState(state, detail = {}) {
    this.state = state;
    this.dispatchEvent(new CustomEvent('state', { detail: { state, ...detail } }));
  }

  _emitProgress() {
    const meta = this._meta ?? this._resume;
    this.dispatchEvent(
      new CustomEvent('progress', {
        detail: { loaded: this._receivedBytes, total: meta ? meta.size : 0 },
      }),
    );
  }

  _adoptResume(resume) {
    if (!resume ||
        !Array.isArray(resume._chunks) ||
        resume._chunks.length !== resume.nextChunk) {
      throw new Error('无效的续传身份：前缀不是完整连续块');
    }
    this._resume = resume;
    this.id = resume.id;
    this._meta = {
      id: resume.id,
      name: resume.name,
      size: resume.size,
      chunks: resume.totalChunks,
      hash: resume.hash,
    };
    this._chunks = resume._chunks ?? [];
    resume._chunks = undefined;
    this._receivedBytes = prefixBytesForSequence(resume.size, resume.nextChunk);
    this.state = 'resumable';
  }

  /** 用户在新连接上点击“恢复本次传输”。 */
  resume() {
    if (this.state !== 'resumable') throw new Error('当前状态不能恢复传输');
    if (this._channel.readyState !== 'open') throw new Error('通道未打开，无法恢复');
    const identity = this._resume;
    this._generation += 1;
    this._setState('resuming', { id: identity.id });
    this._channel.send(encodeResume(identity.toRequest()));
  }

  /**
   * 旧连接断开后暂存连续前缀，并脱离旧通道。
   * @returns {Promise<ReceiverResume|null>}
   */
  async suspendForReconnect() {
    if (this._suspended) return null;
    this._suspended = true;

    if (this.state === 'resumable') {
      const snapshot = this._buildResume(this._resume.prefixHash);
      this.destroy();
      return snapshot;
    }
    if (!['active', 'verifying', 'resuming', 'interrupted'].includes(this.state)) {
      this.destroy();
      return null;
    }

    const identity = this._resume ?? this._meta;
    const expectedNext = this._chunks.length;
    this._generation += 1; // 立即作废旧 end/hash 等异步路径
    this._setState('interrupted', {
      id: this.id,
      name: identity.name,
      size: identity.size,
    });

    let prefixHash;
    try {
      const blob = new Blob(this._chunks, { type: 'application/octet-stream' });
      prefixHash = await sha256(blob);
    } finally {
      this.destroy();
    }

    return this._buildResume(prefixHash, expectedNext);
  }

  _buildResume(prefixHash, nextChunk = this._resume?.nextChunk ?? this._chunks.length) {
    const meta = this._meta;
    if (!meta) return null;
    const chunks = this._chunks.slice(0, nextChunk);
    return new ReceiverResume({
      id: this.id,
      name: meta.name,
      size: meta.size,
      totalChunks: meta.chunks,
      hash: meta.hash,
      nextChunk,
      prefixHash,
      savedChunks: chunks,
    });
  }

  _onMessage = async (event) => {
    const data = event.data;

    if (typeof data === 'string') {
      const msg = parseControl(data);
      if (!msg) {
        if (['active', 'hashing', 'verifying', 'resuming'].includes(this.state)) {
          this._fail('收到无法识别的控制消息');
        }
        return;
      }
      if (msg.kind === 'meta') this._handleMeta(msg);
      else if (msg.kind === 'resume-accept') this._handleResumeAccept(msg);
      else if (msg.kind === 'resume-reject') this._handleResumeReject(msg);
      else if (msg.kind === 'end') {
        await this._handleEnd(msg);
      } else if (msg.kind === 'cancel') this._handleCancel(msg);
      else if (msg.kind === 'receipt') this._handleReceipt(msg);
      return;
    }

    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data) || data instanceof Blob) {
      await this._handleChunk(data);
    }
  };

  _handleMeta(meta) {
    // 新 meta 显式代表一次新的首次传输：所有不可再信任的旧前缀都必须丢弃。
    this._resetFor(meta.id);
    this._meta = meta;
    this._setState('active', {
      id: meta.id,
      name: meta.name,
      size: meta.size,
      chunks: meta.chunks,
    });
  }

  _resetFor(id) {
    this._generation += 1;
    this.id = id;
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
    this._resume = null;
  }

  async _handleChunk(data) {
    // 只有 active 会话接受数据；空闲/终态/旧连接对象上的迟到块直接丢弃。
    if (this.state !== 'active') return;

    let buffer = data;
    if (ArrayBuffer.isView(data)) {
      buffer = data.buffer.slice(data.byteOffset, data.byteOffset + data.byteLength);
    } else if (data instanceof Blob) {
      buffer = await data.arrayBuffer();
    }
    const generation = this._generation;
    const unpacked = unpackChunk(buffer);
    if (!unpacked) {
      this._failWith(generation, '收到损坏的数据块（头部不足或为空）');
      return;
    }
    const { sequence, payload } = unpacked;
    const meta = this._meta;
    if (!meta || sequence >= meta.chunks) return;

    // 只暂存从 0 开始、边界完整的连续前缀。可靠有序通道上乱序意味着旧数据/坏协议，
    // 绝不能把带洞集合伪装成可校验前缀。
    const expectedSequence = this._chunks.length;
    if (sequence !== expectedSequence) return;

    const expectedSize =
      sequence === meta.chunks - 1
        ? meta.size - sequence * CHUNK_SIZE
        : CHUNK_SIZE;
    if (payload.byteLength !== expectedSize) {
      this._failWith(
        generation,
        `数据块 #${sequence} 大小不符（收到 ${payload.byteLength}，应为 ${expectedSize}）`,
      );
      return;
    }

    // 复制一份，避免底层缓冲复用造成串改（测试替身尤其需要）。
    const owned = new Uint8Array(payload.byteLength);
    owned.set(payload);
    if (generation !== this._generation || this.state !== 'active') return;

    this._chunks.push(owned);
    this._receivedBytes += owned.byteLength;
    this._emitProgress();
  }

  _handleResumeAccept(msg) {
    if (this.state !== 'resuming' || msg.id !== this.id) return;
    const identity = this._resume;
    const generation = this._generation;
    if (!identity ||
      msg.nextChunk !== identity.nextChunk ||
      msg.prefixHash !== identity.prefixHash) {
      this._failWith(generation, '发送方接受了错误的续传边界');
      return;
    }
    this._setState('active', {
      id: this.id,
      name: identity.name,
      size: identity.size,
      chunks: identity.totalChunks,
      resumed: true,
    });
    this._emitProgress();
  }

  _handleResumeReject(msg) {
    if (msg.id !== this.id || !['resumable', 'resuming'].includes(this.state)) return;
    this._discard();
    this._setState('failed', { reason: `发送方拒绝恢复：${msg.reason}`, id: msg.id });
  }

  async _handleEnd(msg) {
    // 迟到的 end（不属于当前会话/当前状态）不能完成新传输。
    if (this.state !== 'active' || msg.id !== this.id) return;
    const generation = this._generation;
    const meta = this._meta;

    if (msg.hash !== meta.hash) {
      this._failWith(generation, 'end 消息哈希与 meta 不一致');
      return;
    }
    if (this._chunks.length !== meta.chunks) {
      this._failWith(generation, '块序号不完整');
      return;
    }
    if (this._receivedBytes !== meta.size) {
      this._failWith(generation, '总字节数与声明大小不符');
      return;
    }

    this._setState('verifying');
    let blob;
    let hash;
    try {
      blob = new Blob(this._chunks.length ? this._chunks : [new Uint8Array(0)], {
        type: 'application/octet-stream',
      });
      hash = await sha256(blob);
    } catch (err) {
      this._failWith(generation, `校验失败：${err.message || err}`);
      return;
    }

    // 异步校验期间可能已取消/关闭/被新传输取代：结果必须作废。
    if (generation !== this._generation || this.state !== 'verifying' || msg.id !== this.id) {
      return;
    }
    if (hash !== meta.hash) {
      this._failWith(generation, 'SHA-256 不匹配');
      return;
    }

    // 先把“校验通过”的完整回执写入通道，发送方凭此且仅凭此报告成功；
    // 回执自身都无法发送时，本次传输仍按失败处理，避免两端状态分叉。
    try {
      this._channel.send(encodeReceipt({ id: this.id, ok: true, hash }));
    } catch (err) {
      this._failWith(generation, `发送完成回执失败：${err.message || err}`);
      return;
    }

    this._setState('completed', {
      id: this.id,
      name: meta.name,
      size: meta.size,
      hash,
      blob,
    });
  }

  _handleCancel(msg) {
    // 只取消当前匹配的传输；旧会话的迟到 cancel 不能影响新传输。
    if (msg.id !== this.id) return;
    if (['active', 'verifying', 'resuming', 'resumable'].includes(this.state)) {
      this._discard();
      this._setState('canceled', { reason: '对端取消了传输', id: msg.id });
    }
  }

  /** 接收方不会等待对端回执；对端迟到/串号的 receipt 一律忽略。 */
  _handleReceipt() {}

  /** 接收方主动取消，或用户显式丢弃可恢复前缀。 */
  cancel(reason = '用户取消') {
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    const id = this.id;
    try {
      if (['active', 'verifying', 'resuming'].includes(this.state) &&
          this._channel.readyState === 'open' && id) {
        this._channel.send(encodeCancel({ id }));
      }
    } catch {
      // 以本地状态为准
    }
    this._discard();
    this._setState('canceled', { reason, id });
  }

  fail(reason) {
    this._fail(reason);
  }

  _fail(reason) {
    this._failWith(this._generation, reason);
  }

  /** 仅当代际匹配时失败，避免旧异步路径把新传输打成失败。 */
  _failWith(generation, reason) {
    if (generation !== this._generation) return;
    if (TERMINAL_STATES.has(this.state)) return;

    const id = this.id;
    try {
      if (this._channel.readyState === 'open' && id) {
        if (this.state === 'active' || this.state === 'verifying') {
          this._channel.send(encodeReceipt({ id, ok: false, reason }));
        } else if (this.state === 'resuming') {
          this._channel.send(encodeResumeReject({ id, reason }));
        }
      }
    } catch {
      // 回执通道也不可用时，至少本地必须保留明确失败状态。
    }

    this._discard();
    this._setState('failed', { reason, id });
  }

  _discard() {
    this._generation += 1;
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
    this._resume = null;
  }

  _onChannelClose = () => {
    if (['active', 'verifying', 'resuming'].includes(this.state)) {
      // 不把可恢复断线判成失败；暂存连续前缀，等待用户在新连接显式恢复。
      this._generation += 1;
      const { id } = this;
      const name = this._meta?.name ?? this._resume?.name;
      const size = this._meta?.size ?? this._resume?.size;
      this._setState('interrupted', { id, name, size });
    }
  };

  _onChannelError = () => {
    this._onChannelClose();
  };
}

export { SEND_HIGH_WATER, SEND_LOW_WATER };
