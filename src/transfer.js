// 文件传输核心：FileSender 与 FileReceiver。
//
// 设计要点：
//  - 通道可替换：只依赖 RTCDataChannel 形状（send / bufferedAmount /
//    bufferedAmountLowThreshold / readyState / 事件 / close），便于注入测试替身。
//  - 背压：发送方按 bufferedAmount 高低水位暂停读取与发送，绝不一次塞满。
//  - 代际守卫：每次传输有独立 id 与本地 generation 令牌；取消、关闭、重连/
//    新传输会提升代际，旧传输迟到的事件与异步结果一律作废，不能完成新传输。
//  - 失败必须显式：任何一步不通都进入 'failed' / 'canceled'，不会伪造完成。

import {
  CHUNK_SIZE,
  MAX_FILE_SIZE,
  createTransferId,
  sha256,
  encodeMeta,
  encodeEnd,
  encodeReceipt,
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

export class FileSender extends EventTarget {
  /**
   * @param channel 已 open 的 DataChannel（或测试替身）
   * @param {object}  [opts]
   * @param {number}  [opts.lowThreshold]     缓冲低水位（字节），默认 64 KiB
   * @param {number}  [opts.perChunkDelay]    每发一块后的等待毫秒数（仅测试用）
   * @param {number}  [opts.receiptTimeoutMs] 等待接收方完成回执的超时毫秒，默认 30000；0 表示不超时
   */
  constructor(channel, {
    lowThreshold = SEND_LOW_WATER,
    perChunkDelay = 0,
    receiptTimeoutMs = DEFAULT_RECEIPT_TIMEOUT_MS,
  } = {}) {
    super();
    this._channel = channel;
    this._generation = 0;
    this._drainWaiters = []; // 等待缓冲回落的 Promise（取消/关闭时必须能抢占）
    this._receiptWaiters = []; // 等待接收方校验回执（失败回执必须能终止发送方状态）
    this._receiptTimer = null;
    this._receiptTimeoutMs = receiptTimeoutMs;
    this._perChunkDelay = perChunkDelay;
    this.state = 'idle';
    this.id = null;
    this.file = null;
    this._sentBytes = 0;
    channel.bufferedAmountLowThreshold = lowThreshold;
    channel.addEventListener('close', this._onChannelClose);
    channel.addEventListener('error', this._onChannelError);
    channel.addEventListener('message', this._onMessage);
  }

  /** 解除对通道的监听（彻底放弃该发送器时调用）。 */
  destroy() {
    const ch = this._channel;
    ch.removeEventListener?.('close', this._onChannelClose);
    ch.removeEventListener?.('error', this._onChannelError);
    ch.removeEventListener?.('message', this._onMessage);
  }

  get sentBytes() {
    return this._sentBytes;
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
   * 发送一个文件。
   * @returns 传输 id；校验失败或通道不可用时 throw（状态进入 failed）。
   */
  async start(file) {
    if (['hashing', 'active', 'confirming'].includes(this.state)) {
      throw new Error('已有传输正在进行');
    }
    const channel = this._channel;
    if (channel.readyState !== 'open') {
      this._setState('failed', { reason: '通道未打开' });
      throw new Error('通道未打开，无法发送');
    }
    if (!file || typeof file.size !== 'number') {
      this._setState('failed', { reason: '无效的文件' });
      throw new Error('无效的文件');
    }
    if (file.size > MAX_FILE_SIZE) {
      const reason = `文件超过 ${MAX_FILE_SIZE} 字节上限`;
      this._setState('failed', { reason });
      throw new Error(reason);
    }

    const generation = ++this._generation;
    this.file = file;
    this.id = createTransferId();
    this._sentBytes = 0;
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
      const chunks = Math.ceil(file.size / CHUNK_SIZE);
      channel.send(encodeMeta({ id: this.id, name: file.name, size: file.size, chunks, hash }));
      await this._pump(generation);
      // pump 正常返回代表全部块已写入通道且当前代际仍有效。
      if (generation === this._generation && this.state === 'active') {
        channel.send(encodeEnd({ id: this.id, hash }));
        this._setState('confirming', { id: this.id, hash });
        await this._waitForReceipt(generation, hash);
      }
    } catch (err) {
      if (generation === this._generation && !TERMINAL_STATES.has(this.state)) {
        this._setState('failed', { reason: err.message || String(err) });
      }
    }
    return this.id;
  }

  /** 逐块读取文件并按背压写入通道。 */
  async _pump(generation) {
    const channel = this._channel;
    const file = this.file;
    const highWater = highWaterOf(channel);
    let offset = 0;
    let sequence = 0;
    while (offset < file.size) {
      if (generation !== this._generation) {
        throw new Error('传输已被取代（取消或重连）');
      }
      if (channel.readyState !== 'open') {
        throw new Error('通道已关闭，发送中断');
      }

      // 背压：通道积压达到高水位时，等 bufferedAmountlow 事件，
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
          this._rejectReceiptWaiters('等待接收方校验回执超时');
          this.fail('等待接收方校验回执超时，不能确认传输成功');
        }, this._receiptTimeoutMs);
        // 保持事件循环存活：在 Node 测试替身中底层搬运定时器可能是 unref 的，
        // 等待回执期间仍必须继续处理后续消息。
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
    if (TERMINAL_STATES.has(this.state) || this.state === 'idle') return;
    this._generation += 1; // 让进行中的 pump / 异步哈希结果作废
    const id = this.id;
    this._rejectDrainWaiters('传输已取消');
    this._rejectReceiptWaiters('传输已取消');
    try {
      if (this._channel.readyState === 'open' && id) {
        this._channel.send(JSON.stringify({ kind: 'cancel', id }));
      }
    } catch {
      // 通道可能已关闭；取消仍以本地状态为准。
    }
    this._sentBytes = 0;
    this._setState('canceled', { reason, id });
  }

  /** 外部（连接层）通报致命错误。 */
  fail(reason) {
    if (TERMINAL_STATES.has(this.state)) return;
    this._generation += 1; // 抢占任何等待中的循环
    this._rejectDrainWaiters(reason);
    this._rejectReceiptWaiters(reason);
    this._setState('failed', { reason });
  }

  _onChannelClose = () => {
    if (['hashing', 'active', 'confirming'].includes(this.state)) {
      this.fail('DataChannel 已关闭');
    }
  };

  _onChannelError = () => {
    if (['hashing', 'active', 'confirming'].includes(this.state)) {
      this.fail('DataChannel 发生错误');
    }
  };

  _onMessage = (event) => {
    if (typeof event.data !== 'string') return;
    const msg = parseControl(event.data);
    if (!msg) {
      this._failIfRunning('收到无法识别的控制消息');
      return;
    }

    if (msg.kind === 'receipt') this._handleReceipt(msg);
    // 只认当前传输的取消通知；旧传输迟到的 cancel 不能影响新传输。
    if (msg.kind === 'cancel' && msg.id === this.id &&
        (this.state === 'active' || this.state === 'hashing' || this.state === 'confirming')) {
      this._generation += 1;
      this._rejectDrainWaiters('对端取消了传输');
      this._rejectReceiptWaiters('对端取消了传输');
      this._sentBytes = 0;
      this._setState('canceled', { reason: '接收方取消了传输', id: msg.id });
    }
  };

  _failIfRunning(reason) {
    if (['hashing', 'active', 'confirming'].includes(this.state)) this.fail(reason);
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
    this._setState('completed', { id: this.id, hash: msg.hash });
  }
}

export class FileReceiver extends EventTarget {
  /**
   * @param channel 已 open（或即将 open）的 DataChannel / 测试替身
   */
  constructor(channel) {
    super();
    this._channel = channel;
    this._generation = 0;
    this.state = 'idle';
    this.id = null;
    this._chunks = [];
    this._meta = null;
    this._receivedBytes = 0;
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

  /** 丢弃当前半成品并开始一轮新传输（重连/新 meta 时调用）。 */
  _resetFor(id) {
    this._generation += 1; // 使旧的异步校验结果失效
    this.id = id;
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
  }

  _onMessage = async (event) => {
    const data = event.data;

    // 文本控制消息
    if (typeof data === 'string') {
      const msg = parseControl(data);
      if (!msg) {
        // 无法识别的控制消息：若正在传输则判失败，否则忽略噪声。
        if (this.state === 'active' || this.state === 'hashing') {
          this._fail('收到无法识别的控制消息');
        }
        return;
      }
      if (msg.kind === 'meta') this._handleMeta(msg);
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
    });
  }

  async _handleChunk(data) {
    // 只有 active 会话接受数据；空闲/终态下的迟到块直接丢弃。
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

    if (this._chunks[sequence]) {
      // 可靠有序通道不应出现重复；重复块幂等忽略，不当作新增字节。
      return;
    }
    this._chunks[sequence] = owned;
    this._receivedBytes += owned.byteLength;
    this._emitProgress();
  }

  async _handleEnd(msg) {
    // 迟到的 end（不属于当前会话）不能完成新传输。
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
    // 只取消当前匹配的传输；旧会话的迟到 cancel 不能影响新传输。
    if (msg.id === this.id && (this.state === 'active' || this.state === 'verifying')) {
      this._discard();
      this._setState('canceled', { reason: '对端取消了传输', id: msg.id });
    }
  }

  /** 接收方不会等待对端回执；对端迟到/串号的 receipt 一律忽略。 */
  _handleReceipt(msg) {
    if (msg.id === this.id) return;
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

  _discard() {
    this._generation += 1;
    this._chunks = [];
    this._receivedBytes = 0;
    this._meta = null;
  }

  _onChannelClose = () => {
    if (this.state === 'active' || this.state === 'verifying') {
      this._fail('传输过程中通道关闭');
    }
  };

  _onChannelError = () => {
    if (this.state === 'active' || this.state === 'verifying') {
      this._fail('传输过程中通道出错');
    }
  };
}

export { SEND_HIGH_WATER, SEND_LOW_WATER };
