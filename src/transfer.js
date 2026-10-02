// 文件传输核心：FileSender 与 FileReceiver。
//
// 设计要点：
//  - 通道可替换：只依赖 RTCDataChannel 形状（send / bufferedAmount /
//    bufferedAmountLowThreshold / readyState / 事件 / close），便于注入测试替身。
//  - 背压：发送方按 bufferedAmount 高低水位暂停读取与发送，绝不一次塞满。
//  - 显式恢复：断线只保留接收方页面内“边界完整的连续块前缀”；新连接上由接收方
//    声明传输身份、nextChunk 与前缀 SHA-256，发送方核对所选文件身份、总长及前缀
//    字节后，才从 nextChunk 继续。普通首次传输仍从 meta 与块 0 开始。
//  - 尝试代际：attach/start/resume/取消/失败都会推进 generation；传输对象可以换接
//    新通道，但旧通道监听器会解绑，旧通道迟到的数据、end、receipt 均不能完成新尝试。
//  - 失败必须显式：任何一步不通都进入 'failed' / 'canceled'，不会伪造完成。

import {
  CHUNK_SIZE,
  MAX_FILE_SIZE,
  createTransferId,
  sha256,
  encodeMeta,
  encodeResume,
  encodeResumeAck,
  encodeEnd,
  encodeReceipt,
  parseControl,
  packChunk,
  unpackChunk,
  sameTransferIdentity,
  digestChunkPrefix,
} from './protocol.js';

// 默认低水位（字节）。发送方会把 channel.bufferedAmountLowThreshold 设为此值；
// 一旦缓冲达到低水位的 HIGH_WATER_FACTOR 倍即暂停，等 bufferedamountlow 再继续。
const SEND_LOW_WATER = 64 * 1024;
const SEND_HIGH_WATER = 256 * 1024;
const HIGH_WATER_FACTOR = 4;
const DEFAULT_RECEIPT_TIMEOUT_MS = 30_000;
const DEFAULT_RESUME_TIMEOUT_MS = 30_000;

/** 当前通道的高水位：以配置的低水位阈值为准，缺省回落到常量。 */
function highWaterOf(channel) {
  const threshold = channel.bufferedAmountLowThreshold;
  if (typeof threshold === 'number' && threshold > 0) {
    return threshold * HIGH_WATER_FACTOR;
  }
  return SEND_HIGH_WATER;
}

const TERMINAL_STATES = new Set(['completed', 'canceled', 'failed']);
const RUNNING_STATES = ['hashing', 'active', 'confirming'];

export class FileSender extends EventTarget {
  /**
   * @param channel 已 open 的 DataChannel（或测试替身）
   * @param {object}  [opts]
   * @param {number}  [opts.lowThreshold]     缓冲低水位（字节），默认 64 KiB
   * @param {number}  [opts.perChunkDelay]    每发一块后的等待毫秒数（仅测试用）
   * @param {number}  [opts.receiptTimeoutMs] 等待接收方完成回执的超时毫秒，默认 30000；0 表示不超时
   * @param {number}  [opts.resumeTimeoutMs]  等待接收方恢复声明/ack 的超时毫秒，默认 30000
   */
  constructor(channel, {
    lowThreshold = SEND_LOW_WATER,
    perChunkDelay = 0,
    receiptTimeoutMs = DEFAULT_RECEIPT_TIMEOUT_MS,
    resumeTimeoutMs = DEFAULT_RESUME_TIMEOUT_MS,
  } = {}) {
    super();
    this._channel = null;
    this._generation = 0;
    this._drainWaiters = []; // 等待缓冲回落的 Promise（取消/关闭时必须能抢占）
    this._receiptWaiters = []; // 等待接收方校验回执（失败回执必须能终止发送方状态）
    this._resumeWaiter = null;
    this._resumeTimer = null;
    this._pendingResumeOffer = null;
    this._receiptTimer = null;
    this._receiptTimeoutMs = receiptTimeoutMs;
    this._resumeTimeoutMs = resumeTimeoutMs;
    this._perChunkDelay = perChunkDelay;
    this.state = 'idle';
    this.id = null;
    this.file = null;
    this._hash = null;
    this._chunks = 0;
    this._nextChunk = 0;
    this._sentBytes = 0;
    this.attach(channel, lowThreshold);
  }

  /** 换接重连后的通道；旧通道立即解绑，迟到事件只能落在旧监听器之外。 */
  attach(channel, lowThreshold = SEND_LOW_WATER) {
    this.detach();
    this._channel = channel;
    this._generation += 1; // 每次连接都是独立尝试代际
    this._rejectDrainWaiters('已切换到新的 DataChannel');
    this._rejectReceiptWaiters('已切换到新的 DataChannel');
    this._rejectResumeWaiter('已切换到新的 DataChannel');
    channel.bufferedAmountLowThreshold = lowThreshold;
    channel.addEventListener('close', this._onChannelClose);
    channel.addEventListener('error', this._onChannelError);
    channel.addEventListener('message', this._onMessage);
    this._notifyCancelOnOpen(channel);
    if (RUNNING_STATES.includes(this.state)) {
      this._interrupt('DataChannel 已关闭，等待显式恢复');
    }
  }

  /** 通道打开后若本地此前已取消，补发一次取消，让仍保留旧前缀的对端也清理。 */
  _notifyCancelOnOpen(channel) {
    const sendCancel = () => {
      if (this.state === 'canceled' && this.id && channel.readyState === 'open') {
        try { channel.send(JSON.stringify({ kind: 'cancel', id: this.id })); } catch { /* ignore */ }
      }
    };
    if (channel.readyState === 'open') {
      queueMicrotask(sendCancel);
    } else {
      channel.addEventListener('open', sendCancel, { once: true });
    }
  }

  /** 解除当前通道监听（重连或彻底放弃发送器时调用）。 */
  detach() {
    const ch = this._channel;
    if (!ch) return;
    ch.removeEventListener?.('close', this._onChannelClose);
    ch.removeEventListener?.('error', this._onChannelError);
    ch.removeEventListener?.('message', this._onMessage);
  }

  /** 解除对通道的监听（彻底放弃该发送器时调用）。 */
  destroy() {
    this.detach();
    this._rejectDrainWaiters('发送器已销毁');
    this._rejectReceiptWaiters('发送器已销毁');
    this._rejectResumeWaiter('发送器已销毁');
  }

  get channel() {
    return this._channel;
  }

  get generation() {
    return this._generation;
  }

  get sentBytes() {
    return this._sentBytes;
  }

  get nextChunk() {
    return this._nextChunk;
  }

  _identity() {
    if (!this.id || !this.file || !this._hash) return null;
    return {
      id: this.id,
      name: this.file.name,
      size: this.file.size,
      chunks: this._chunks,
      hash: this._hash,
    };
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

  /**
   * 首次发送一个文件。恢复必须显式调用 resume()，不能用新 meta 悄悄复用旧前缀。
   * @returns 传输 id；校验失败或通道不可用时 throw（状态进入 failed）。
   */
  async start(file) {
    if (['hashing', 'active', 'confirming', 'resuming', 'interrupted'].includes(this.state)) {
      throw new Error('已有传输正在进行或等待恢复');
    }
    const invalid = this._validateSelectedFile(file);
    if (invalid) {
      this._clearTransfer();
      this._setState('failed', { reason: invalid });
      throw new Error(invalid);
    }
    if (this._channel.readyState !== 'open') {
      const reason = '通道未打开，无法发送';
      this._clearTransfer();
      this._setState('failed', { reason });
      throw new Error(reason);
    }

    const generation = ++this._generation;
    this.file = file;
    this.id = createTransferId();
    this._hash = null;
    this._chunks = Math.ceil(file.size / CHUNK_SIZE);
    this._nextChunk = 0;
    this._sentBytes = 0;
    this._setState('hashing');

    let hash;
    try {
      hash = await sha256(file);
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._failWith(generation, `计算哈希失败：${err.message || err}`);
      }
      throw err;
    }
    if (generation !== this._generation || TERMINAL_STATES.has(this.state)) return this.id;

    this._hash = hash;
    await this._runAttempt(generation, 0);
    return this.id;
  }

  /**
   * 显式恢复当前逻辑传输。接收方必须先在新通道上发送 resume 声明；发送方重新计算
   * 所选文件整文件哈希，并直接读取文件前缀核对 SHA-256。
   */
  async resume(file = this.file) {
    if (this.state !== 'interrupted') throw new Error('当前没有可恢复的断线传输');
    const previousId = this.id;
    const invalid = this._validateSelectedFile(file);
    if (invalid) {
      this._rejectResumeOffer(previousId, invalid);
      this._failWith(this._generation, invalid);
      throw new Error(invalid);
    }

    const generation = ++this._generation;
    this.state = 'resuming';
    this._setState('resuming');
    try {
      const hash = await sha256(file);
      const offer = await this._waitForResumeOffer(generation);
      const candidate = {
        id: offer.id,
        name: file.name,
        size: file.size,
        chunks: Math.ceil(file.size / CHUNK_SIZE),
        hash,
      };
      if (!sameTransferIdentity(offer, candidate)) {
        throw new Error('恢复身份不匹配：已选择不同文件，不能续传');
      }
      if (offer.nextChunk > candidate.chunks) throw new Error('恢复边界超出文件块数');

      const prefixBytes = offer.nextChunk * CHUNK_SIZE;
      const prefix = new Uint8Array(await file.slice(0, prefixBytes).arrayBuffer());
      const prefixHash = await sha256(prefix);
      if (prefixHash !== offer.prefixHash) {
        throw new Error('前缀字节摘要不匹配，不能信任接收方暂存内容');
      }

      this.file = file;
      this._hash = hash;
      this._chunks = candidate.chunks;
      this._channel.send(encodeResumeAck({
        id: this.id, ok: true, nextChunk: offer.nextChunk, prefixHash,
      }));
      await this._runAttempt(generation, offer.nextChunk);
    } catch (err) {
      const reason = err.message || String(err);
      // 恢复核对期间断线仍是可恢复状态，不能把刚收到的旧前缀当失败清掉。
      if (generation !== this._generation && this.state === 'interrupted') return;
      this._rejectResumeOffer(previousId, reason);
      this._failWith(generation, `恢复失败：${reason}`);
      throw new Error(reason);
    }
  }

  _validateSelectedFile(file) {
    if (!file || typeof file.size !== 'number') return '无效的文件';
    if (file.size > MAX_FILE_SIZE) return `文件超过 ${MAX_FILE_SIZE} 字节上限`;
    if (this._channel?.readyState !== 'open') return '通道未打开，无法发送';
    return '';
  }

  /** 发送 meta（首次）或直接续发，然后发送 end 并等待成功回执。 */
  async _runAttempt(generation, startChunk) {
    const channel = this._channel;
    const file = this.file;
    const hash = this._hash;
    this._nextChunk = startChunk;
    this._sentBytes = startChunk * CHUNK_SIZE;
    this._emitProgress();

    try {
      if (startChunk === 0) {
        channel.send(encodeMeta({
          id: this.id, name: file.name, size: file.size,
          chunks: this._chunks, hash,
        }));
      }
      this._setState('active', { id: this.id, nextChunk: startChunk });
      await this._pump(generation, startChunk);
      // pump 正常返回代表全部剩余块已写入通道且当前代际仍有效。
      if (generation === this._generation && this.state === 'active') {
        channel.send(encodeEnd({ id: this.id, hash }));
        this._setState('confirming', { id: this.id, hash });
        await this._waitForReceipt(generation, hash);
      }
    } catch (err) {
      const reason = err.message || String(err);
      if (generation === this._generation && !TERMINAL_STATES.has(this.state) && channel.readyState !== 'open') {
        // 通道断开是可恢复事件：保留文件身份/接收前缀，等待用户在新连接上显式 resume。
        this._interrupt(reason);
      } else {
        this._failWith(generation, reason);
      }
    }
  }

  /** 从 startSequence 对应字节偏移逐块读取文件并按背压写入通道。 */
  async _pump(generation, startSequence) {
    const channel = this._channel;
    const file = this.file;
    const highWater = highWaterOf(channel);
    let sequence = startSequence;
    let offset = startSequence * CHUNK_SIZE;
    while (offset < file.size) {
      if (generation !== this._generation) {
        throw new Error('传输已被取代（取消或重连）');
      }
      if (channel.readyState !== 'open') {
        throw new Error('通道已关闭，发送中断');
      }

      // 背压：通道积压达到高水位时，等 bufferedamountlow 事件，
      // 期间若关闭/出错/取消则立即醒来并失败，不继续塞数据。
      if (channel.bufferedAmount >= highWater) {
        await this._waitForDrain(generation);
      }

      const end = Math.min(offset + CHUNK_SIZE, file.size);
      const block = new Uint8Array(await file.slice(offset, end).arrayBuffer());
      if (generation !== this._generation) throw new Error('传输已取消');
      if (channel.readyState !== 'open') throw new Error('通道已关闭，发送中断');

      channel.send(packChunk(sequence, block));
      this._nextChunk = sequence + 1;
      this._sentBytes += block.byteLength;
      this._emitProgress();
      offset = end;
      sequence += 1;

      // 每次 send 后立即检查：一旦越线且后面还有块才暂停，绝不在一个循环里继续塞满。
      // 最后一块之后直接进入 end 发送；end 只是很小的控制消息，不能因为错过一个
      // bufferedamountlow 事件而把整个传输永久卡在等待里。
      if (channel.bufferedAmount >= highWater && offset < file.size) {
        await this._waitForDrain(generation);
      } else if (this._perChunkDelay > 0) {
        // 测试钩子：在超快链路上人为放慢每块，制造可取消/断开窗口。
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
      // 双保险：注册期间代际已变化（取消）则立即醒来。
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
          const reason = '等待接收方校验回执超时';
          this._rejectReceiptWaiters(reason);
          this._failWith(generation, `${reason}，不能确认传输成功`);
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

  _waitForResumeOffer(generation) {
    if (this._pendingResumeOffer) return Promise.resolve(this._pendingResumeOffer);
    return new Promise((resolve, reject) => {
      this._resumeWaiter = { generation, resolve, reject };
      if (this._resumeTimeoutMs > 0) {
        this._resumeTimer = setTimeout(() => {
          this._rejectResumeWaiter('等待接收方恢复声明超时');
        }, this._resumeTimeoutMs);
      }
    });
  }

  _resolveResumeWaiter(offer) {
    if (!this._resumeWaiter) return;
    if (this._resumeTimer) clearTimeout(this._resumeTimer);
    this._resumeTimer = null;
    const waiter = this._resumeWaiter;
    this._resumeWaiter = null;
    waiter.resolve(offer);
  }

  _rejectResumeWaiter(reason) {
    if (this._resumeTimer) clearTimeout(this._resumeTimer);
    this._resumeTimer = null;
    const waiter = this._resumeWaiter;
    this._resumeWaiter = null;
    if (waiter) waiter.reject(new Error(reason));
  }

  _rejectResumeOffer(id, reason) {
    if (!id) return;
    try {
      if (this._channel?.readyState === 'open') {
        this._channel.send(encodeResumeAck({ id, ok: false, reason }));
      }
    } catch {
      // 新通道不可用时至少本地明确失败。
    }
  }

  /** 主动取消：通知对端丢弃（尽力而为），本地丢弃半成品。 */
  cancel(reason = '用户取消') {
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    const id = this.id;
    this._generation += 1; // 让进行中的 pump / 异步哈希结果作废
    this._rejectDrainWaiters('传输已取消');
    this._rejectReceiptWaiters('传输已取消');
    this._rejectResumeWaiter('传输已取消');
    this._pendingResumeOffer = null;
    try {
      if (this._channel?.readyState === 'open' && id) {
        this._channel.send(JSON.stringify({ kind: 'cancel', id }));
      }
    } catch {
      // 通道可能已关闭；取消仍以本地状态为准。
    }
    this.file = null;
    this._hash = null;
    this._chunks = 0;
    this._nextChunk = 0;
    this._sentBytes = 0;
    // 保留 id 一小段时间用于拒绝同页面新通道上迟到/迟到一步的恢复声明。
    this.id = id;
    this._setState('canceled', { reason, id });
  }

  /** 外部（连接层）在不可恢复场景通报致命错误。 */
  fail(reason) {
    this._failWith(this._generation, reason);
  }

  /** 断线不丢身份：等待用户显式恢复；用户取消/校验失败才清掉半成品。 */
  _interrupt(reason) {
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    this._generation += 1;
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._rejectResumeWaiter(reason);
    this._setState('interrupted', { reason, id: this.id });
  }

  _failWith(generation, reason) {
    if (generation !== this._generation) return;
    if (TERMINAL_STATES.has(this.state)) return;
    this._generation += 1; // 抢占任何等待中的循环
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._rejectResumeWaiter(reason);
    this._pendingResumeOffer = null;
    this.file = null;
    this._hash = null;
    this._chunks = 0;
    this._nextChunk = 0;
    this._sentBytes = 0;
    this._setState('failed', { reason });
  }

  _clearTransfer() {
    this.file = null;
    this.id = null;
    this._hash = null;
    this._chunks = 0;
    this._nextChunk = 0;
    this._sentBytes = 0;
    this._pendingResumeOffer = null;
  }

  _onChannelClose = () => {
    if (RUNNING_STATES.includes(this.state) || this.state === 'resuming') {
      this._interrupt('DataChannel 已关闭，可在新连接上恢复本次传输');
    }
  };

  _onChannelError = () => {
    if (RUNNING_STATES.includes(this.state) || this.state === 'resuming') {
      this._interrupt('DataChannel 发生错误，可在新连接上恢复本次传输');
    }
  };

  _onMessage = (event) => {
    if (event.target !== undefined && event.target !== this._channel) return;
    if (typeof event.data !== 'string') return;
    const msg = parseControl(event.data);
    if (!msg) {
      this._failIfRunning('收到无法识别的控制消息');
      return;
    }

    if (msg.kind === 'receipt') this._handleReceipt(msg);
    else if (msg.kind === 'resume') this._handleResumeOffer(msg);
    // 只认当前传输的取消通知；旧传输迟到的 cancel 不能影响新传输。
    else if (msg.kind === 'cancel' && msg.id === this.id &&
        ['active', 'hashing', 'confirming', 'resuming', 'interrupted'].includes(this.state)) {
      const reason = '对端取消了传输';
      this._generation += 1;
      this._rejectDrainWaiters(reason);
      this._rejectReceiptWaiters(reason);
      this._rejectResumeWaiter(reason);
      this._pendingResumeOffer = null;
      this.file = null;
      this._hash = null;
      this._chunks = 0;
      this._nextChunk = 0;
      this._sentBytes = 0;
      this._setState('canceled', { reason, id: msg.id });
    }
  };

  _failIfRunning(reason) {
    if (RUNNING_STATES.includes(this.state) || this.state === 'resuming') this._failWith(this._generation, reason);
  }

  _handleResumeOffer(offer) {
    // 发送方已取消：明确拒绝，让接收方清掉前缀，而不是让其等到超时。
    if (this.state === 'canceled' && offer.id === this.id) {
      this._rejectResumeOffer(offer.id, '发送方已取消本次传输');
      return;
    }
    if (this.state !== 'interrupted' && this.state !== 'resuming') return;
    if (this.id !== offer.id) return;
    this._pendingResumeOffer = offer;
    this._resolveResumeWaiter(offer);
  }

  _handleReceipt(msg) {
    // 只处理当前传输的回执；旧 id / 旧通道的迟到回执不能改变新传输。
    if (msg.id !== this.id) return;
    if (!['active', 'confirming'].includes(this.state)) return;

    if (!msg.ok) {
      this._failWith(this._generation, `接收方校验失败：${msg.reason}`);
      return;
    }
    if (this.state === 'active') {
      // 可靠有序通道上，成功回执不可能先于 end；收到即协议不一致。
      this._failWith(this._generation, '接收方在 end 前发送了完成回执');
      return;
    }

    // 回执必须绑定完整文件哈希；只收到“成功”二字不算可核对的完成。
    const waiter = this._receiptWaiters.find((w) => w.generation === this._generation);
    if (!waiter || msg.hash !== waiter.expectedHash) {
      this._failWith(this._generation, '接收方完成回执的哈希不一致');
      return;
    }

    this._resolveReceiptWaiters({ hash: msg.hash });
    this._setState('completed', { id: this.id, hash: msg.hash });
  }
}

export class FileReceiver extends EventTarget {
  /**
   * @param channel 已 open（或即将 open）的 DataChannel / 测试替身
   */
  constructor(channel) {
    super();
    this._channel = null;
    this._generation = 0;
    this.state = 'idle';
    this.id = null;
    this._chunks = [];
    this._meta = null;
    this._receivedBytes = 0;
    this._resumeTimer = null;
    this._requestedResume = null;
    this.attach(channel);
  }

  /** 换接重连后的通道；旧通道解绑，且本次连接获得新的尝试代际。 */
  attach(channel) {
    this.detach();
    this._channel = channel;
    this._generation += 1;
    this._clearResumeTimer();
    if (this.state === 'resuming' || RUNNING_STATES.includes(this.state)) {
      this._interrupt('DataChannel 已关闭，可显式恢复本次传输');
    }
    channel.addEventListener('message', this._onMessage);
    channel.addEventListener('close', this._onChannelClose);
    channel.addEventListener('error', this._onChannelError);
    this._notifyCancelOnOpen(channel);
  }

  detach() {
    const ch = this._channel;
    if (!ch) return;
    ch.removeEventListener?.('message', this._onMessage);
    ch.removeEventListener?.('close', this._onChannelClose);
    ch.removeEventListener?.('error', this._onChannelError);
  }

  /** 通道打开后若本地此前已取消，补发一次取消，让仍保留旧传输的对端停止。 */
  _notifyCancelOnOpen(channel) {
    const sendCancel = () => {
      if (this.state === 'canceled' && this.id && channel.readyState === 'open') {
        try { channel.send(JSON.stringify({ kind: 'cancel', id: this.id })); } catch { /* ignore */ }
      }
    };
    if (channel.readyState === 'open') {
      queueMicrotask(sendCancel);
    } else {
      channel.addEventListener('open', sendCancel, { once: true });
    }
  }

  destroy() {
    this.detach();
    this._clearResumeTimer();
  }

  get channel() {
    return this._channel;
  }

  get generation() {
    return this._generation;
  }

  get receivedBytes() {
    return this._receivedBytes;
  }

  get nextChunk() {
    return this._continuousPrefixChunks();
  }

  get prefixChunks() {
    return this.nextChunk;
  }

  _setState(state, detail = {}) {
    this.state = state;
    this.dispatchEvent(new CustomEvent('state', { detail: { state, ...detail } }));
  }

  _emitProgress() {
    const meta = this._meta;
    this.dispatchEvent(
      new CustomEvent('progress', {
        detail: { loaded: this._receivedBytes, total: meta ? meta.size : 0 },
      }),
    );
  }

  /** 仅统计从 0 开始、边界完整的连续块前缀；空洞后的迟到块不计入可恢复字节。 */
  _continuousPrefixChunks() {
    let next = 0;
    while (next < this._chunks.length && this._chunks[next] instanceof Uint8Array) next += 1;
    return next;
  }

  /** 丢弃当前半成品并开始一轮首次传输（新 meta 时调用）。 */
  _resetFor(id) {
    this._clearResumeTimer();
    this._generation += 1; // 使旧的异步校验结果失效
    this.id = id;
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
    this._requestedResume = null;
  }

  _onMessage = async (event) => {
    if (event.target !== undefined && event.target !== this._channel) return;
    const data = event.data;

    // 文本控制消息
    if (typeof data === 'string') {
      const msg = parseControl(data);
      if (!msg) {
        // 无法识别的控制消息：若正在传输则判失败，否则忽略噪声。
        if (this.state === 'active' || this.state === 'hashing' || this.state === 'resuming') {
          this._fail('收到无法识别的控制消息');
        }
        return;
      }
      if (msg.kind === 'meta') this._handleMeta(msg);
      else if (msg.kind === 'resume') this._handleUnexpectedResumeOffer(msg);
      else if (msg.kind === 'resume-ack') await this._handleResumeAck(msg);
      else if (msg.kind === 'end') await this._handleEnd(msg);
      else if (msg.kind === 'cancel') this._handleCancel(msg);
      else if (msg.kind === 'receipt') this._handleReceipt(msg);
      return;
    }

    // 二进制数据块
    if (data instanceof ArrayBuffer || ArrayBuffer.isView(data) || data instanceof Blob) {
      await this._handleChunk(data);
    }
  };

  _handleMeta(meta) {
    // 新 meta 一律丢弃旧传输的半成品（无论旧传输处于什么状态）。
    this._resetFor(meta.id);
    this._meta = meta;
    this._setState('active', {
      id: meta.id,
      name: meta.name,
      size: meta.size,
      chunks: meta.chunks,
      nextChunk: 0,
    });
  }

  async _handleChunk(data) {
    // 只有 active 会话接受数据；空闲/终态/等待恢复下的迟到块直接丢弃。
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
      this._failWith(generation, `收到损坏的数据块（头部不足或为空）`);
      return;
    }
    const { sequence, payload } = unpacked;
    const meta = this._meta;
    if (!meta || sequence >= meta.chunks) {
      // 序号超出当前传输范围：无法区分是损坏还是旧传输迟到的块，
      // 按无关数据忽略——绝不能让旧传输的迟到事件完成或搞坏新传输。
      return;
    }

    // 只暂存从 0 开始的连续下一块。可靠有序通道上，乱序/空洞后的块不能伪装成前缀。
    const expected = this._continuousPrefixChunks();
    if (sequence !== expected) return;

    // 块大小必须符合预期（中间块满块，最后一块对齐总大小）。
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
    if (generation !== this._generation || this.state !== 'active') return; // 已被取代

    this._chunks[sequence] = owned;
    this._receivedBytes += owned.byteLength;
    this._emitProgress();
  }

  /** 用户在新连接上显式要求恢复：声明身份、下一块序号与连续前缀摘要。 */
  async resume() {
    if (this.state !== 'interrupted') throw new Error('当前没有可恢复的断线接收任务');
    const meta = this._meta;
    const generation = ++this._generation;
    const nextChunk = this._continuousPrefixChunks();
    let prefixHash;
    try {
      prefixHash = await digestChunkPrefix(this._chunks, nextChunk);
    } catch (err) {
      this._failWith(generation, `计算恢复前缀失败：${err.message || err}`);
      throw err;
    }
    if (generation !== this._generation || this.state !== 'interrupted') return;

    this._requestedResume = { nextChunk, prefixHash };
    this._setState('resuming', { id: this.id, nextChunk, prefixHash });
    try {
      this._channel.send(encodeResume({
        id: this.id, name: meta.name, size: meta.size, chunks: meta.chunks,
        hash: meta.hash, nextChunk, prefixHash,
      }));
    } catch (err) {
      this._failWith(generation, `发送恢复声明失败：${err.message || err}`);
      return;
    }

    if (this._resumeTimer) clearTimeout(this._resumeTimer);
    this._resumeTimer = setTimeout(() => {
      if (this._generation === generation && this.state === 'resuming') {
        this._failWith(generation, '等待发送方恢复确认超时');
      }
    }, DEFAULT_RESUME_TIMEOUT_MS);
  }

  async _handleResumeAck(msg) {
    if (this.state !== 'resuming' || msg.id !== this.id) return;
    const requested = this._requestedResume;
    if (!requested) {
      this._failWith(this._generation, '恢复确认没有对应的恢复请求');
      return;
    }
    if (!msg.ok) {
      this._failWith(this._generation, `发送方拒绝恢复：${msg.reason}`);
      return;
    }
    if (msg.nextChunk !== requested.nextChunk || msg.prefixHash !== requested.prefixHash) {
      this._failWith(this._generation, '恢复确认的边界或前缀摘要不匹配');
      return;
    }
    this._clearResumeTimer();
    this._requestedResume = null;
    this._setState('active', {
      id: this.id,
      name: this._meta.name,
      size: this._meta.size,
      chunks: this._meta.chunks,
      nextChunk: msg.nextChunk,
    });
    this._emitProgress();
  }

  async _handleEnd(msg) {
    // 迟到的 end（不属于当前会话/当前尝试）不能完成新传输。
    if (this.state !== 'active' || msg.id !== this.id) return;
    const generation = this._generation;
    const meta = this._meta;

    if (msg.hash !== meta.hash) {
      this._failWith(generation, 'end 消息哈希与 meta 不一致');
      return;
    }
    if (this._chunks.length !== meta.chunks || this._chunks.some((c) => !c)) {
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
    // 只取消当前匹配的传输；旧会话的迟到 cancel 不能影响新连接上的另一个传输。
    if (msg.id === this.id &&
        ['active', 'verifying', 'resuming', 'interrupted'].includes(this.state)) {
      this._discard();
      this._setState('canceled', { reason: '对端取消了传输', id: msg.id });
    }
  }

  /** 接收方不会等待对端回执；对端迟到/串号的 receipt 一律忽略。 */
  _handleReceipt(msg) {
    if (msg.id === this.id) return;
  }

  /** 接收方不应收到 resume 控制消息；同代际协议错误必须显式失败。 */
  _handleUnexpectedResumeOffer(msg) {
    if (msg.id === this.id && ['active', 'resuming'].includes(this.state)) {
      this._fail('收到方向错误的恢复声明');
    }
  }

  /** 接收方主动取消：通知发送方并丢弃半成品。 */
  cancel(reason = '用户取消') {
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    const id = this.id;
    try {
      if (this._channel.readyState === 'open' && id) {
        this._channel.send(JSON.stringify({ kind: 'cancel', id }));
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

    // 明确把本地校验结果通知发送方；发送方不得仅凭 end 已写完就报告成功。
    const id = this.id;
    try {
      if (this._channel.readyState === 'open' && id) {
        this._channel.send(encodeReceipt({ id, ok: false, reason }));
      }
    } catch {
      // 回执通道也不可用时，至少本地必须保留明确失败状态。
    }

    this._discard();
    this._setState('failed', { reason, id });
  }

  _interrupt(reason) {
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    this._generation += 1;
    this._clearResumeTimer();
    this._requestedResume = null;
    this._setState('interrupted', {
      reason,
      id: this.id,
      nextChunk: this._continuousPrefixChunks(),
    });
  }

  _clearResumeTimer() {
    if (this._resumeTimer !== null) {
      clearTimeout(this._resumeTimer);
      this._resumeTimer = null;
    }
    this._requestedResume = null;
  }

  _discard() {
    this._generation += 1;
    this._clearResumeTimer();
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
  }

  _onChannelClose = () => {
    if (this.state === 'active' || this.state === 'verifying' || this.state === 'resuming') {
      this._interrupt('传输过程中通道关闭');
    }
  };

  _onChannelError = () => {
    if (this.state === 'active' || this.state === 'verifying' || this.state === 'resuming') {
      this._interrupt('传输过程中通道出错');
    }
  };
}

export { SEND_HIGH_WATER, SEND_LOW_WATER };
