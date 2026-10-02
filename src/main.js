// 页面入口：手工信令 + 传输 UI 编排。
//
// 关键不变式：
//  - 重新连接会关闭旧 PeerConnection，并为新通道创建独立 attempt 的传输对象；
//  - 接收方连续前缀只在当前页面内存中随显式 resume 身份交接给新对象；
//  - 只有接收方完成 大小/序号/SHA-256 全部校验后才出现下载链接；
//  - 任何失败都显示明确原因，绝不出现“假装传输完成”。

import { createOffer, createAnswer, acceptAnswer, monitorConnection, waitForChannel, DEFAULT_ICE_SERVERS } from './signaling.js';
import { FileSender, FileReceiver } from './transfer.js';
import { createTransferId, MAX_FILE_SIZE, packChunk, encodeEnd, encodeReceipt } from './protocol.js';

// 同机/离线测试可用 ?ice=host 只用 host 候选，避免等待不可达的公网 STUN。
const ICE_SERVERS = new URLSearchParams(location.search).get('ice') === 'host' ? [] : DEFAULT_ICE_SERVERS;

const $ = (id) => document.getElementById(id);

const els = {
  role: document.querySelectorAll('input[name="role"]'),
  restart: $('restart'),
  offerBtn: $('offer-btn'),
  answerBtn: $('answer-btn'),
  acceptBtn: $('accept-btn'),
  signalIn: $('signal-in'),
  signalInLabel: $('signal-in-label'),
  signalOut: $('signal-out'),
  signalOutLabel: $('signal-out-label'),
  copyRow: $('copy-row'),
  copyOut: $('copy-out'),
  connState: $('conn-state'),
  connDetail: $('conn-detail'),
  connError: $('conn-error'),
  transferPanel: $('transfer-panel'),
  fileInput: $('file-input'),
  sendBtn: $('send-btn'),
  cancelSend: $('cancel-send'),
  discardSenderResume: $('discard-sender-resume'),
  senderBox: $('sender-box'),
  receiverBox: $('receiver-box'),
  recvMeta: $('recv-meta'),
  cancelRecv: $('cancel-recv'),
  resumeRecv: $('resume-recv'),
  discardResume: $('discard-resume'),
  resumeHint: $('resume-hint'),
  xferState: $('xfer-state'),
  xferDetail: $('xfer-detail'),
  progressWrap: $('progress-wrap'),
  progressBar: $('progress-bar'),
  progressText: $('progress-text'),
  xferError: $('xfer-error'),
  xferWarn: $('xfer-warn'),
  downloadBox: $('download-box'),
  hashBox: $('hash-box'),
};

const STATE_LABELS = {
  idle: '空闲',
  hashing: '计算哈希中',
  resuming: '恢复核对中',
  active: '传输中',
  confirming: '等待接收校验',
  verifying: '校验中',
  interrupted: '连接中断，可恢复',
  resumable: '等待恢复',
  completed: '完成',
  canceled: '已取消',
  failed: '失败',
};

const session = {
  role: 'offer',
  pc: null,
  channel: null,
  sender: null,
  receiver: null,
  stopMonitor: null,
  downloadUrl: null,
  attemptId: null,
  resume: null,
  awaitingResume: false,
  detachedAttempts: [],
};

function selectedRole() {
  return document.querySelector('input[name="role"]:checked').value;
}

function setConnBadge(state, text) {
  els.connState.className = `badge ${state}`;
  els.connState.textContent = text;
}

function showConnError(message) {
  els.connError.hidden = false;
  els.connError.textContent = message;
}

function clearConnError() {
  els.connError.hidden = true;
  els.connError.textContent = '';
}

function setXferBadge(stateKey, detail = '') {
  els.xferState.className = `badge ${stateKey}`;
  els.xferState.textContent = STATE_LABELS[stateKey] ?? stateKey;
  els.xferDetail.textContent = detail;
}

function showXferError(message) {
  els.xferError.hidden = false;
  els.xferError.textContent = `失败原因：${message}`;
}

function showXferWarn(message) {
  els.xferWarn.hidden = false;
  els.xferWarn.textContent = message;
}

function clearTransferFeedback() {
  els.xferError.hidden = true;
  els.xferError.textContent = '';
  els.xferWarn.hidden = true;
  els.xferWarn.textContent = '';
}

function clearDownload() {
  if (session.downloadUrl) {
    URL.revokeObjectURL(session.downloadUrl);
    session.downloadUrl = null;
  }
  els.downloadBox.hidden = true;
  els.downloadBox.innerHTML = '';
  els.hashBox.hidden = true;
  els.hashBox.textContent = '';
}

function closeConnectionTransport() {
  if (session.stopMonitor) {
    session.stopMonitor();
    session.stopMonitor = null;
  }
  try {
    session.channel?.close?.();
  } catch {
    // ignore
  }
  try {
    session.pc?.close?.();
  } catch {
    // ignore
  }
  session.channel = null;
  session.pc = null;
}

/**
 * 用户准备交换新的 SDP：旧传输对象脱离旧通道，但接收方连续前缀仍留在本页面。
 * 必须在关闭底层连接前取回快照。
 */
async function detachConnectionForReconnect(message = '重新连接') {
  clearDownload();
  let receiverResume = null;
  try {
    receiverResume = await session.receiver?.suspendForReconnect?.();
  } catch {
    receiverResume = null;
  }
  session.sender?.suspendForReconnect?.(message);
  session.sender?.destroy?.();
  session.receiver?.destroy?.();
  const oldSender = session.sender;
  const oldReceiver = session.receiver;
  if (oldSender || oldReceiver) {
    session.detachedAttempts.push({
      attemptId: session.attemptId,
      sender: oldSender,
      receiver: oldReceiver,
    });
    session.detachedAttempts = session.detachedAttempts.slice(-5);
  }
  session.sender = null;
  session.receiver = null;
  session.attemptId = null;
  if (receiverResume) {
    session.resume = receiverResume;
    session.awaitingResume = false;
  } else if (oldSender?.file &&
      ['active', 'confirming', 'resuming', 'interrupted'].includes(oldSender.state)) {
    session.awaitingResume = true;
  }
  closeConnectionTransport();
}

/** 最终放弃当前连接和任何可恢复前缀（切换角色、测试重置）。 */
async function teardownConnection(message) {
  clearDownload();
  if (session.stopMonitor) {
    session.stopMonitor();
    session.stopMonitor = null;
  }
  for (const t of [session.sender, session.receiver]) {
    try {
      t?.fail?.(message || '连接已重置');
    } catch {
      // ignore
    }
    try {
      t?.destroy?.();
    } catch {
      // ignore
    }
  }
  session.sender = null;
  session.receiver = null;
  session.resume = null;
  session.awaitingResume = false;
  session.detachedAttempts = [];
  session.attemptId = null;
  closeConnectionTransport();
  els.transferPanel.hidden = true;
}

function formatBytes(n) {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
  return `${(n / (1024 * 1024)).toFixed(2)} MiB`;
}

function setProgress(loaded, total) {
  if (!total) {
    els.progressWrap.hidden = true;
    els.progressText.textContent = '';
    return;
  }
  els.progressWrap.hidden = false;
  const pct = Math.min(100, (loaded / total) * 100);
  els.progressBar.style.width = `${pct.toFixed(1)}%`;
  els.progressText.textContent = `${formatBytes(loaded)} / ${formatBytes(total)}（${pct.toFixed(1)}%）`;
}

function resetView() {
  clearConnError();
  clearTransferFeedback();
  setConnBadge('idle', '未连接');
  els.connDetail.textContent = '';
  els.signalIn.value = '';
  els.signalOut.value = '';
  els.signalIn.hidden = true;
  els.signalInLabel.hidden = true;
  els.signalOut.hidden = true;
  els.signalOutLabel.hidden = true;
  els.copyRow.hidden = true;
  setXferBadge('idle');
  setProgress(0, 0);
  els.fileInput.value = '';
  els.sendBtn.disabled = true;
  els.cancelSend.hidden = true;
  els.discardSenderResume.hidden = true;
  els.cancelRecv.hidden = true;
  els.resumeRecv.hidden = true;
  els.resumeRecv.disabled = false;
  els.discardResume.hidden = true;
  els.resumeHint.hidden = true;
  els.resumeHint.textContent = '';
  els.recvMeta.textContent = '等待文件元信息…';
}

async function resetAll(message) {
  await teardownConnection(message);
  resetView();
  applyRoleView();
}

function renderRecoverableTransfer() {
  els.transferPanel.hidden = false;
  clearDownload();
  clearTransferFeedback();
  const resume = session.resume;
  if (resume) {
    setXferBadge('resumable');
    els.receiverBox.hidden = false;
    els.recvMeta.innerHTML = `待恢复文件：<code></code>`;
    els.recvMeta.querySelector('code').textContent = resume.name;
    els.recvMeta.append(` · ${formatBytes(resume.size)}`);
    const prefix = resume.prefixBytes;
    setProgress(prefix, resume.size);
    els.resumeHint.hidden = false;
    els.resumeHint.textContent =
      `已暂存 ${formatBytes(prefix)} 连续前缀（下一块 #${resume.nextChunk}）。建立新连接后点击恢复。`;
    els.cancelRecv.hidden = true;
    els.discardResume.hidden = false;
    els.resumeRecv.hidden = true; // 新通道打开后启用
  } else if (session.awaitingResume) {
    const file = els.fileInput.files[0];
    if (file) {
      setXferBadge('interrupted', '等待接收方恢复');
      setProgress(session.sender?.sentBytes ?? 0, file.size);
      showXferWarn('发送中断：请保持同一文件已选择；新连接上由接收方发起恢复。');
    }
  }
  els.discardSenderResume.hidden = !(resume || session.awaitingResume);
}

function resetConnectionView() {
  clearConnError();
  clearTransferFeedback();
  setConnBadge('idle', '未连接');
  els.connDetail.textContent = '';
  els.signalIn.value = '';
  els.signalOut.value = '';
  els.signalOut.hidden = true;
  els.signalOutLabel.hidden = true;
  els.copyRow.hidden = true;
  els.offerBtn.disabled = false;
  els.answerBtn.disabled = false;
  els.acceptBtn.hidden = true;
  els.acceptBtn.disabled = false;
  applyRoleView();
  renderRecoverableTransfer();
}

function applyRoleView() {
  session.role = selectedRole();
  if (session.role === 'offer') {
    els.offerBtn.hidden = false;
    els.answerBtn.hidden = true;
    els.acceptBtn.hidden = true;
    els.signalInLabel.textContent = '粘贴应答方回传的文本：';
  } else {
    els.offerBtn.hidden = true;
    els.answerBtn.hidden = false;
    els.acceptBtn.hidden = true;
    els.signalInLabel.textContent = '粘贴发起方的连接信息：';
  }
  // 粘贴框对两种角色一开始就可见；输出框在生成文本后才显示。
  els.signalIn.hidden = false;
  els.signalInLabel.hidden = false;
}

function wireTransferObjects() {
  const { sender, receiver } = session;

  sender.addEventListener('state', (e) => {
    const { state, reason } = e.detail;
    setXferBadge(state, state === 'failed' && reason ? reason : '');
    clearTransferFeedback();
    if (['hashing', 'active', 'confirming', 'resuming'].includes(state)) {
      session.awaitingResume = false;
      els.cancelSend.hidden = false;
      els.sendBtn.disabled = true;
      els.fileInput.disabled = true;
      els.discardSenderResume.hidden = true;
    }
    if (state === 'completed') {
      els.cancelSend.hidden = true;
      els.fileInput.disabled = false;
      els.fileInput.value = '';
      els.sendBtn.disabled = false;
      els.discardSenderResume.hidden = true;
      setProgress(sender.file?.size ?? 0, sender.file?.size ?? 0);
    }
    if (state === 'failed') {
      els.cancelSend.hidden = true;
      els.fileInput.disabled = false;
      els.sendBtn.disabled = false;
      els.discardSenderResume.hidden = true;
      if (reason) showXferError(reason);
    }
    if (state === 'canceled') {
      els.cancelSend.hidden = true;
      els.fileInput.disabled = false;
      els.sendBtn.disabled = false;
      els.discardSenderResume.hidden = true;
      setProgress(0, 0);
      showXferWarn('已取消：本地及对端的半成品均被丢弃。');
    }
    if (state === 'interrupted') {
      els.cancelSend.hidden = true;
      els.fileInput.disabled = false;
      els.sendBtn.disabled = true;
      els.discardSenderResume.hidden = false;
      setProgress(sender.sentBytes, sender.file?.size ?? 0);
      showXferWarn('连接中断：重新交换连接信息后，由接收方显式恢复；请保留同一文件选择。');
    }
  });
  sender.addEventListener('progress', (e) => {
    setProgress(e.detail.loaded, e.detail.total);
  });

  receiver.addEventListener('state', (e) => {
    const { state, reason, name, size, hash, blob } = e.detail;
    clearTransferFeedback();
    setXferBadge(state, state === 'failed' && reason ? reason : '');
    els.receiverBox.hidden = false;
    if (state === 'resumable') {
      const info = session.receiver.info;
      els.recvMeta.innerHTML = `待恢复文件：<code></code>`;
      els.recvMeta.querySelector('code').textContent = info.name;
      els.recvMeta.append(` · ${formatBytes(info.size)}`);
      setProgress(session.receiver.receivedBytes, info.size);
      els.resumeHint.hidden = false;
      els.resumeHint.textContent = '新连接已打开；点击恢复并等待发送方核对前缀。';
      els.resumeRecv.hidden = false;
      els.resumeRecv.disabled = session.channel?.readyState !== 'open';
      els.discardResume.hidden = false;
      els.cancelRecv.hidden = true;
      return;
    }
    if (state === 'resuming') {
      const info = session.receiver.info;
      els.recvMeta.innerHTML = `正在恢复：<code></code>`;
      els.recvMeta.querySelector('code').textContent = info?.name ?? '';
      setProgress(session.receiver.receivedBytes, info?.size ?? 0);
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      els.cancelRecv.hidden = false;
      els.resumeHint.hidden = false;
      els.resumeHint.textContent = '已声明原传输身份、下一块序号与前缀摘要，正在核对…';
      return;
    }
    if (state === 'active') {
      els.recvMeta.innerHTML = `接收文件：<code></code>`;
      els.recvMeta.querySelector('code').textContent = name;
      els.recvMeta.append(` · ${formatBytes(size)}`);
      const startSize = session.receiver.receivedBytes;
      setProgress(startSize, size);
      clearDownload();
      session.resume = null;
      els.cancelRecv.hidden = false;
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      els.resumeHint.hidden = true;
    }
    if (state === 'verifying') {
      setProgress(session.receiver.receivedBytes, session.receiver.info?.size ?? 0);
    }
    if (state === 'interrupted') {
      const info = session.receiver.info;
      els.recvMeta.innerHTML = `连接中断：<code></code>`;
      els.recvMeta.querySelector('code').textContent = info?.name ?? '';
      if (info) els.recvMeta.append(` · ${formatBytes(info.size)}`);
      setProgress(session.receiver.receivedBytes, info?.size ?? 0);
      els.cancelRecv.hidden = true;
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      showXferWarn('连接中断。点击“重新连接”，交换新连接信息后可恢复已暂存的连续前缀。');
    }
    if (state === 'completed') {
      setProgress(size, size);
      els.cancelRecv.hidden = true;
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      els.resumeHint.hidden = true;
      session.resume = null;
      // 仅在大小、序号、哈希全部校验通过后才产生下载链接。
      session.downloadUrl = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = session.downloadUrl;
      a.download = name;
      a.className = 'download';
      a.textContent = `下载 ${name}（${formatBytes(size)}，SHA-256 已校验）`;
      els.downloadBox.replaceChildren(a);
      els.downloadBox.hidden = false;
      els.hashBox.textContent = `SHA-256: ${hash}`;
      els.hashBox.hidden = false;
    }
    if (state === 'failed') {
      setProgress(0, 0);
      clearDownload();
      session.resume = null;
      els.cancelRecv.hidden = true;
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      els.resumeHint.hidden = true;
      if (reason) showXferError(reason);
    }
    if (state === 'canceled') {
      setProgress(0, 0);
      clearDownload();
      session.resume = null;
      els.cancelRecv.hidden = true;
      els.resumeRecv.hidden = true;
      els.discardResume.hidden = true;
      els.resumeHint.hidden = true;
      showXferWarn('传输已取消，已丢弃不可再信任的半成品。');
    }
  });
  receiver.addEventListener('progress', (e) => {
    setProgress(e.detail.loaded, e.detail.total);
  });
}

function attachChannel(channel) {
  session.channel = channel;

  const openIfReady = () => {
    if (channel.readyState !== 'open') return;
    setConnBadge('connected', '已连接（DataChannel 已打开）');
    els.connDetail.textContent = '';
    els.transferPanel.hidden = false;

    // 每个新连接都创建新的传输对象和独立 attemptId；旧通道上的迟到事件只发给
    // 已脱离的旧对象，不能完成本次新尝试。续传身份通过内存快照显式交给新对象。
    if (!session.sender) {
      session.attemptId = createTransferId();
      const perChunkDelay = Number(new URLSearchParams(location.search).get('chunkDelay')) || 0;
      const resume = session.resume;
      session.sender = new FileSender(channel, {
        attemptId: session.attemptId,
        perChunkDelay,
        getResumeFile: () => els.fileInput.files[0] ?? null,
      });
      session.receiver = new FileReceiver(channel, { attemptId: session.attemptId, resume });
      wireTransferObjects();
    }
    els.senderBox.hidden = false;
    els.sendBtn.disabled = !els.fileInput.files[0] || Boolean(session.resume) || session.awaitingResume;
    els.discardSenderResume.hidden = !session.resume && !session.awaitingResume;
    if (session.resume) {
      els.resumeRecv.hidden = false;
      els.resumeRecv.disabled = false;
      els.discardResume.hidden = false;
      els.resumeHint.hidden = false;
      renderRecoverableTransfer();
      // renderRecoverableTransfer 在连接建立前把恢复按钮藏起；通道现已打开。
      els.resumeRecv.hidden = false;
      els.resumeRecv.disabled = false;
    }
  };

  channel.addEventListener('open', openIfReady);
  openIfReady(); // 可能已经 open
  channel.addEventListener('close', () => {
    setConnBadge('idle', '通道已关闭');
  });
}

async function onOffer() {
  clearConnError();
  try {
    setConnBadge('connecting', '生成中…');
    const { pc, channel, payload } = await createOffer({ iceServers: ICE_SERVERS });
    session.pc = pc;
    session.stopMonitor = monitorConnection(pc, {
      onFailed: (err) => {
        setConnBadge('failed', '');
        showConnError(`${err.message}；请重新连接以恢复或重发`);
        detachConnectionForReconnect(err.message).then(() => resetConnectionView());
      },
      onClosed: () => {
        setConnBadge('idle', '连接已关闭');
      },
    });
    attachChannel(channel);

    els.signalOut.value = payload;
    els.signalOut.hidden = false;
    els.signalOutLabel.hidden = false;
    els.signalOutLabel.textContent = '复制下面的连接信息，发给应答方：';
    els.copyRow.hidden = false;
    els.signalIn.hidden = false;
    els.signalInLabel.hidden = false;
    els.signalInLabel.textContent = '粘贴应答方回传的文本：';
    els.offerBtn.disabled = true;
    els.acceptBtn.hidden = false;
    setConnBadge('connecting', '等待应答…');
  } catch (err) {
    setConnBadge('failed', '');
    showConnError(`生成连接信息失败：${err.message || err}`);
  }
}

async function onAnswer() {
  clearConnError();
  const offerText = els.signalIn.value.trim();
  if (!offerText) {
    showConnError('请先粘贴发起方的连接信息。');
    return;
  }
  try {
    setConnBadge('connecting', '生成应答中…');
    const { pc, payload } = await createAnswer(offerText, { iceServers: ICE_SERVERS });
    session.pc = pc;
    session.stopMonitor = monitorConnection(pc, {
      onFailed: (err) => {
        setConnBadge('failed', '');
        showConnError(`${err.message}；请重新连接以恢复或重发`);
        detachConnectionForReconnect(err.message).then(() => resetConnectionView());
      },
      onClosed: () => {
        setConnBadge('idle', '连接已关闭');
      },
    });

    els.signalIn.hidden = true;
    els.signalInLabel.hidden = true;
    els.signalOut.value = payload;
    els.signalOut.hidden = false;
    els.signalOutLabel.hidden = false;
    els.signalOutLabel.textContent = '复制下面的应答信息，发回发起方（之后等待连接建立）：';
    els.copyRow.hidden = false;
    els.answerBtn.disabled = true;
    setConnBadge('connecting', '应答已生成，复制回发起方后等待连接…');

    // datachannel 要等发起方接受 answer 后才出现；在后台等待并接入。
    waitForChannel(pc)
      .then((channel) => {
        // 用户可能已点“重置”，期间 session.pc 已换走。
        if (session.pc === pc) attachChannel(channel);
      })
      .catch((err) => {
        if (session.pc === pc) {
          setConnBadge('failed', '');
          showConnError(err.message || String(err));
        }
      });
  } catch (err) {
    setConnBadge('failed', '');
    showConnError(err.message || String(err));
  }
}

async function onAccept() {
  clearConnError();
  const answerText = els.signalIn.value.trim();
  if (!answerText) {
    showConnError('请先粘贴应答方回传的文本。');
    return;
  }
  if (!session.pc) {
    showConnError('当前没有进行中的连接，请先点「生成连接信息」。');
    return;
  }
  try {
    setConnBadge('connecting', '接受应答中…');
    await acceptAnswer(session.pc, answerText);
    els.acceptBtn.disabled = true;
  } catch (err) {
    setConnBadge('failed', '');
    showConnError(err.message || String(err));
  }
}

async function onSend() {
  const file = els.fileInput.files[0];
  if (!file) return;
  if (!session.sender || !['idle', 'failed', 'canceled', 'completed'].includes(session.sender.state)) return;
  clearTransferFeedback();
  clearDownload();
  if (file.size > MAX_FILE_SIZE) {
    // 明确失败：进入失败状态并给出原因，绝不静默或假装已发送。
    setXferBadge('failed');
    showXferError(`文件 ${formatBytes(file.size)} 超过 ${formatBytes(MAX_FILE_SIZE)} 上限，未发送。`);
    return;
  }
  try {
    await session.sender.start(file);
  } catch (err) {
    // start 自身已经进入 failed 并显示原因；此处无需重复。
  }
}

async function copyOut() {
  try {
    await navigator.clipboard.writeText(els.signalOut.value);
    els.copyOut.textContent = '已复制';
    setTimeout(() => (els.copyOut.textContent = '复制文本'), 1200);
  } catch {
    els.signalOut.select();
    document.execCommand?.('copy');
  }
}

function sendStaleChunkToOldReceiver(sequence, payload) {
  const attempt = session.detachedAttempts.at(-1);
  if (!attempt?.receiver) throw new Error('没有旧接收尝试');
  attempt.receiver._onMessage({ data: packChunk(sequence, payload) });
}

function sendStaleEndToOldReceiver(hash) {
  const attempt = session.detachedAttempts.at(-1);
  if (!attempt?.receiver) throw new Error('没有旧接收尝试');
  attempt.receiver._onMessage({ data: encodeEnd({ id: attempt.receiver.id, hash }) });
}

function sendStaleReceiptToOldSender(hash) {
  const attempt = session.detachedAttempts.at(-1);
  if (!attempt?.sender) throw new Error('没有旧发送尝试');
  attempt.sender._onMessage({ data: encodeReceipt({ id: attempt.sender.id, ok: true, hash }) });
}

// ---- 事件绑定 ----
els.role.forEach((r) => r.addEventListener('change', () => {
  resetAll('切换角色');
}));
els.restart.addEventListener('click', async () => {
  await detachConnectionForReconnect('手动重新连接');
  resetConnectionView();
});
els.offerBtn.addEventListener('click', onOffer);
els.answerBtn.addEventListener('click', onAnswer);
els.acceptBtn.addEventListener('click', onAccept);
els.copyOut.addEventListener('click', copyOut);
els.fileInput.addEventListener('change', () => {
  clearTransferFeedback();
  const f = els.fileInput.files[0];
  els.sendBtn.disabled = !f || Boolean(session.resume);
  if (f && f.size > MAX_FILE_SIZE) {
    showXferError(`文件 ${formatBytes(f.size)} 超过 ${formatBytes(MAX_FILE_SIZE)} 上限。`);
  }
});
els.sendBtn.addEventListener('click', onSend);
els.cancelSend.addEventListener('click', () => session.sender.cancel());
els.cancelRecv.addEventListener('click', () => session.receiver?.cancel('接收方拒收'));
els.resumeRecv.addEventListener('click', () => {
  try {
    session.receiver?.resume();
  } catch (err) {
    showXferError(err.message || String(err));
  }
});
els.discardResume.addEventListener('click', () => {
  session.receiver?.cancel('用户放弃可恢复前缀');
  session.resume = null;
  els.resumeRecv.hidden = true;
  els.discardResume.hidden = true;
  els.resumeHint.hidden = true;
  els.sendBtn.disabled = !els.fileInput.files[0];
});
els.discardSenderResume.addEventListener('click', () => {
  session.resume = null;
  session.awaitingResume = false;
  els.discardSenderResume.hidden = true;
  els.sendBtn.disabled = !els.fileInput.files[0];
  setXferBadge('idle');
  showXferWarn('已放弃续传；可选择同一文件或新文件重新发送。');
});

applyRoleView();

// 供页面自动化测试使用的钩子（不影响正常使用）。
window.__app = {
  session,
  MAX_FILE_SIZE,
  reset: () => resetAll('测试重置'),
  sendStaleChunkToOldReceiver,
  sendStaleEndToOldReceiver,
  sendStaleReceiptToOldSender,
};
