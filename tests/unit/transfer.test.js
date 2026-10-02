import { test } from 'node:test';
import assert from 'node:assert/strict';
import { FileSender, FileReceiver, ReceiverResume } from '../../src/transfer.js';
import { FakePair, FakeDataChannel } from '../../src/fake-channel.js';
import { CHUNK_SIZE, sha256, packChunk, encodeMeta, encodeEnd, encodeReceipt } from '../../src/protocol.js';

// ---------- 辅助 ----------

const states = (t) => t.__states ??= [];

function record(t) {
  t.addEventListener('state', (e) => {
    states(t).push(e.detail.state);
  });
}

function waitForState(t, target, { timeoutMs = 10000 } = {}) {
  return new Promise((resolve, reject) => {
    if (t.state === target) return resolve();
    const timer = setTimeout(() => reject(new Error(`等待状态 ${target} 超时（当前 ${t.state}）`)), timeoutMs);
    const on = (e) => {
      if (e.detail.state === target || ['failed', 'canceled', 'completed'].includes(e.detail.state)) {
        clearTimeout(timer);
        t.removeEventListener('state', on);
        if (e.detail.state === target) resolve(e.detail);
        else reject(new Error(`等到的是 ${e.detail.state}${e.detail.reason ? `：${e.detail.reason}` : ''}`));
      }
    };
    t.addEventListener('state', on);
  });
}

function collectDetails(t) {
  const details = [];
  t.addEventListener('state', (e) => details.push(e.detail));
  return details;
}

async function settle(ms = 30) {
  await new Promise((r) => setTimeout(r, ms));
}

async function readBlob(blob) {
  return new Uint8Array(await blob.arrayBuffer());
}

// ---------- 正常路径 ----------

test('正常传输：大小、序号、哈希全部匹配后 completed 并产出 Blob', async () => {
  const pair = new FakePair({ bytesPerTick: 64 * 1024 });
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);
  record(sender);
  record(receiver);

  const size = CHUNK_SIZE * 3 + 123;
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = (i * 7) & 0xff;
  const file = new Blob([bytes]);
  file.name = 'hello.bin';

  const details = collectDetails(receiver);
  const done = waitForState(receiver, 'completed');
  const id = await sender.start(file);
  const result = await done;

  assert.equal(result.id, id);
  assert.equal(result.name, 'hello.bin');
  assert.equal(result.size, size);
  assert.equal(result.blob.size, size);
  const got = await readBlob(result.blob);
  assert.ok(got.every((b, i) => b === bytes[i]), '内容逐字节一致');
  assert.equal(result.hash, await sha256(bytes));
  assert.ok(states(sender).includes('completed'));
  assert.ok(states(receiver).includes('verifying'));
  assert.equal(receiver.state, 'completed');
  pair.drop();
});

test('空文件也能完成（0 字节）', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);
  const file = new Blob([]);
  file.name = 'empty';
  const done = waitForState(receiver, 'completed');
  await sender.start(file);
  const result = await done;
  assert.equal(result.size, 0);
  assert.equal(result.blob.size, 0);
  assert.equal(result.hash,
    'e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855');
  pair.drop();
});

test('发送方超过 5 MiB 直接失败，不向通道写任何数据', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  let sent = 0;
  const origSend = pair.a.send.bind(pair.a);
  pair.a.send = (d) => { sent += typeof d === 'string' ? d.length : d.byteLength; return origSend(d); };

  const file = new Blob([new Uint8Array(5 * 1024 * 1024 + 1)]);
  file.name = 'too-big';
  await assert.rejects(() => sender.start(file), /上限/);
  assert.equal(sender.state, 'failed');
  assert.equal(sent, 0);
  pair.drop();
});

// ---------- 背压 / 缓冲回落 ----------

test('背压：高水位时暂停发送，bufferedamountlow 后继续，不会一次塞满', async () => {
  // 很慢的链路：每 tick 只消费 1 个块；低水位 1 块、高水位 4 块，确保频繁等待。
  const pair = new FakePair({
    highWaterMark: 4 * CHUNK_SIZE,
    lowThreshold: CHUNK_SIZE,
    bytesPerTick: CHUNK_SIZE,
  });
  // 发送方低水位与假通道对齐，其高水位 = 低水位 * 4 = 4 个块。
  const sender = new FileSender(pair.a, { lowThreshold: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b);

  let maxObserved = 0;
  let pauseCount = 0;
  const highWater = 4 * CHUNK_SIZE;
  const origSend = pair.a.send.bind(pair.a);
  pair.a.send = (d) => {
    origSend(d);
    maxObserved = Math.max(maxObserved, pair.a.bufferedAmount);
    if (pair.a.bufferedAmount >= highWater) pauseCount++;
  };

  const size = CHUNK_SIZE * 60;
  const file = new Blob([new Uint8Array(size)]);
  file.name = 'backpressure.bin';
  const done = waitForState(receiver, 'completed', { timeoutMs: 20000 });
  await sender.start(file);
  await done;

  // 背压生效：每个发送循环只在水位线附近发，不会无界增长。
  assert.ok(maxObserved < 8 * CHUNK_SIZE,
    `观察到的最大缓冲 ${maxObserved} 不应远超高水位`);
  assert.ok(pauseCount >= 1, '应至少因背压暂停过一次');
  assert.equal(sender.sentBytes, size);
  pair.drop();
});

// ---------- 断开 ----------

test('传输中断开通道：双方进入可恢复中断状态，不会 completed', async () => {
  const pair = new FakePair({
    highWaterMark: 4 * CHUNK_SIZE,
    lowThreshold: CHUNK_SIZE,
    bytesPerTick: CHUNK_SIZE,
  });
  const sender = new FileSender(pair.a, { lowThreshold: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b);
  record(sender);
  record(receiver);

  // 200 块、每秒约 1000 块（1ms/tick）的慢速链路上，20ms 内绝不可能传完。
  const size = CHUNK_SIZE * 200;
  const file = new Blob([new Uint8Array(size)]);
  file.name = 'drop.bin';
  const startPromise = sender.start(file);
  // 等哈希完成、进入 active，再让若干块上路，然后断开（消除哈希耗时抖动）。
  await waitForState(sender, 'active');
  await settle(20);
  assert.equal(sender.state, 'active');
  pair.drop();
  await startPromise.catch(() => {});
  await settle(10);

  assert.equal(sender.state, 'interrupted');
  assert.notEqual(sender.state, 'completed');
  assert.notEqual(receiver.state, 'completed');
  assert.ok(['interrupted', 'active'].includes(receiver.state),
    `接收方应可恢复或仍在传输，实际 ${receiver.state}`);
});

test('未传输时断开不产生假完成；之后新传输仍可工作（重连语义）', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);
  pair.drop();
  await settle(5);
  assert.notEqual(receiver.state, 'completed');

  // 用新的一对通道模拟重连（旧对象已随旧通道作废）。
  const pair2 = new FakePair();
  const sender2 = new FileSender(pair2.a);
  const receiver2 = new FileReceiver(pair2.b);
  const file = new Blob([new Uint8Array(CHUNK_SIZE * 2 + 5)]);
  file.name = 'after-reconnect';
  const done = waitForState(receiver2, 'completed');
  await sender2.start(file);
  const result = await done;
  assert.equal(result.blob.size, file.size);
  pair2.drop();
});

// ---------- 坏块 ----------

test('坏块：载荷大小与序号位置不符 -> 接收方失败且不提供下载', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);

  // 篡改：把序号 1 的块截短（meta 声明 3 个满块，大小不再自洽）
  pair.setTransform((from, message) => {
    if (message.kind === 'binary' && from === pair.a) {
      const seq = new DataView(message.data).getUint32(0, false);
      if (seq === 1) {
        return { kind: 'binary', data: message.data.slice(0, 4 + 10), size: 14 };
      }
    }
    return message;
  });

  const file = new Blob([new Uint8Array(CHUNK_SIZE * 3)]);
  file.name = 'bad-size.bin';
  const startPromise = sender.start(file);
  const detail = await waitForState(receiver, 'failed');
  assert.match(detail.reason, /大小不符/);
  await assert.rejects(startPromise).catch(() => {});
  await settle(5);
  assert.equal(receiver.state, 'failed');
  pair.drop();
});

test('坏块：块头损坏（不足 5 字节）-> 失败', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);
  pair.setTransform((from, message) => {
    if (message.kind === 'binary' && from === pair.a) {
      return { kind: 'binary', data: new ArrayBuffer(3), size: 3 };
    }
    return message;
  });
  const file = new Blob([new Uint8Array(CHUNK_SIZE)]);
  file.name = 'bad-head.bin';
  const startPromise = sender.start(file);
  const detail = await waitForState(receiver, 'failed');
  assert.match(detail.reason, /损坏/);
  await startPromise.catch(() => {});
  pair.drop();
});

test('内容被改导致哈希不符 -> 验证阶段失败，绝不 completed', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);

  // 翻转最后一个块的一个字节（大小/序号仍完整）
  pair.setTransform((from, message) => {
    if (message.kind === 'binary' && from === pair.a) {
      const copy = message.data.slice(0);
      new Uint8Array(copy)[copy.byteLength - 1] ^= 0xff;
      return { kind: 'binary', data: copy, size: copy.byteLength };
    }
    return message;
  });

  const file = new Blob([new Uint8Array(CHUNK_SIZE * 2 + 7)]);
  file.name = 'tampered.bin';
  const p = sender.start(file);
  const detail = await waitForState(receiver, 'failed');
  assert.match(detail.reason, /SHA-256/);
  const sdetail = await waitForState(sender, 'failed');
  assert.match(sdetail.reason, /接收方校验失败[\s\S]*SHA-256/);
  await p.catch(() => {});
  pair.drop();
});

test('完成回执：发送方只有收到 ok 且 hash 匹配的 receipt 后才 completed', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a, { receiptTimeoutMs: 1_000 });
  const receiver = new FileReceiver(pair.b);

  const file = new Blob([new Uint8Array(CHUNK_SIZE + 11).fill(5)]);
  file.name = 'ack.bin';
  let heldReceipt = null;
  pair.setTransform((from, message) => {
    if (from === pair.b && message.kind === 'text' && message.data.includes('"receipt"')) {
      heldReceipt = message;
      return null; // 先截获回执，确认发送方不会凭 end 自行完成
    }
    return message;
  });

  const startPromise = sender.start(file);
  await waitForState(receiver, 'completed');
  await settle(10);
  assert.equal(sender.state, 'confirming', '回执到达前不能报告成功');

  pair.setTransform(null);
  pair.b._pair.a._deliver(heldReceipt);
  await startPromise;
  assert.equal(sender.state, 'completed');
  pair.drop();
});

test('缺少成功回执：发送方超时失败，不把 end 已发送当作完成', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a, { receiptTimeoutMs: 20 });
  const receiver = new FileReceiver(pair.b);

  // 丢弃接收方发出的回执控制消息。
  pair.setTransform((from, message) => {
    if (from === pair.b && message.kind === 'text') return null;
    return message;
  });

  const file = new Blob([new Uint8Array(CHUNK_SIZE).fill(6)]);
  file.name = 'no-ack.bin';
  const details = collectDetails(sender);
  await sender.start(file);
  await waitForState(sender, 'failed').catch(() => {});
  const failure = details.find((d) => d.state === 'failed');
  assert.match(failure.reason, /回执超时/);
  assert.equal(receiver.state, 'completed');
  pair.drop();
});

test('成功回执 hash 不匹配：内容不完整的完成确认不能让发送方成功', async () => {
  const pair = new FakePair();
  const sender = new FileSender(pair.a, { receiptTimeoutMs: 1_000 });
  const receiver = new FileReceiver(pair.b);
  pair.setTransform((from, message) => {
    if (from === pair.b && message.kind === 'text' && message.data.includes('"receipt"')) {
      return { kind: 'text', data: message.data.replace(/"hash":"[0-9a-f]+"/, '"hash":"00"'), size: 0 };
    }
    return message;
  });

  const file = new Blob([new Uint8Array(CHUNK_SIZE).fill(8)]);
  file.name = 'bad-ack.bin';
  const p = sender.start(file);
  const detail = await waitForState(sender, 'failed');
  assert.match(detail.reason, /回执[\s\S]*哈希/);
  await p.catch(() => {});
  pair.drop();
});

// ---------- 取消竞争 ----------

test('取消竞争：发送方在背压等待中取消，start 不挂起，双方丢弃半成品', async () => {
  const pair = new FakePair({
    highWaterMark: 3 * CHUNK_SIZE,
    lowThreshold: CHUNK_SIZE,
    bytesPerTick: CHUNK_SIZE, // 很慢
  });
  const sender = new FileSender(pair.a, { lowThreshold: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b);
  record(sender);
  record(receiver);

  const file = new Blob([new Uint8Array(CHUNK_SIZE * 100)]);
  file.name = 'cancel-mid.bin';
  const startPromise = sender.start(file);
  // 等到确认进入 active（哈希是异步的，不能在 hashing 阶段断言）。
  await waitForState(sender, 'active');
  await settle(5); // 再让若干块上路，进入背压等待
  sender.cancel();

  await startPromise; // 必须正常返回而不是挂起
  assert.equal(sender.state, 'canceled');
  const rdetail = await waitForState(receiver, 'canceled');
  assert.match(rdetail.reason, /取消/);
  assert.equal(receiver.receivedBytes, 0, '半成品被丢弃');
  pair.drop();
});

test('接收方取消：通知发送方，已收块被丢弃，后续迟到块不能完成传输', async () => {
  const pair = new FakePair({
    highWaterMark: 8 * CHUNK_SIZE,
    lowThreshold: 2 * CHUNK_SIZE,
    bytesPerTick: CHUNK_SIZE,
  });
  const sender = new FileSender(pair.a);
  const receiver = new FileReceiver(pair.b);

  const file = new Blob([new Uint8Array(CHUNK_SIZE * 30)]);
  file.name = 'recv-cancel.bin';
  const startPromise = sender.start(file);
  await settle(15);
  receiver.cancel('不想要了');
  await settle(30); // 让取消消息到达，链路把剩余块继续投递一段时间

  assert.equal(receiver.state, 'canceled');
  assert.notEqual(receiver.state, 'completed');
  assert.equal(receiver.receivedBytes, 0);
  // 发送方收到 cancel 后也应停下（失败或取消），不能假装完成
  await startPromise.catch(() => {});
  assert.ok(['failed', 'canceled'].includes(sender.state),
    `发送方应失败/取消，实际 ${sender.state}`);
  pair.drop();
});

// ---------- 重连 / 迟到事件不能完成新传输 ----------

test('新 meta 丢弃旧传输半成品；旧传输迟到的 end/块不能完成新传输', async () => {
  const pair = new FakePair({ bytesPerTick: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b); // 接收方挂在 b
  const fromA = (data) => pair.a.send(data);  // 从对端 a 发送

  const size1 = CHUNK_SIZE * 4;
  const id1 = 'old-transfer';
  const bytes1 = new Uint8Array(size1).fill(1);
  const hash1 = await sha256(bytes1);
  fromA(encodeMeta({ id: id1, name: 'old.bin', size: size1, chunks: 4, hash: hash1 }));
  pair.tick(); // 假通道由 tick 异步投递
  assert.equal(receiver.state, 'active');
  // 收到旧传输的两个块
  for (let seq = 0; seq < 2; seq++) {
    fromA(packChunk(seq, new Uint8Array(CHUNK_SIZE).fill(1)));
  }
  pair.tick(); // 块 0 进入在途
  pair.tick(); // 块 0 送达、块 1 进入在途
  pair.tick(); // 块 1 送达
  assert.equal(receiver.receivedBytes, 2 * CHUNK_SIZE);

  // 新传输开始：半成品必须清空
  const size2 = CHUNK_SIZE;
  const bytes2 = new Uint8Array(size2).fill(9);
  const hash2 = await sha256(bytes2);
  const id2 = 'new-transfer';
  fromA(encodeMeta({ id: id2, name: 'new.bin', size: size2, chunks: 1, hash: hash2 }));
  pair.tick();
  pair.tick(); // 等在途清空后新 meta 到达，旧半成品被丢弃
  assert.equal(receiver.receivedBytes, 0);
  assert.equal(receiver.id, id2);

  // 旧传输迟到的块与 end：必须被无视，不能完成也不能搞坏新传输
  fromA(packChunk(2, new Uint8Array(CHUNK_SIZE).fill(1)));
  fromA(encodeEnd({ id: id1, hash: hash1 }));
  pair.tick();
  assert.equal(receiver.state, 'active');
  assert.equal(receiver.receivedBytes, 0);

  // 新传输正常完成
  const done = waitForState(receiver, 'completed');
  fromA(packChunk(0, bytes2));
  fromA(encodeEnd({ id: id2, hash: hash2 }));
  pair.tick();
  const result = await done;
  assert.equal(result.id, id2);
  assert.equal(result.name, 'new.bin');
  pair.drop();
});

test('终态之后迟到的消息不会改变状态', async () => {
  const pair = new FakePair();
  const receiver = new FileReceiver(pair.b);
  const size = CHUNK_SIZE;
  const bytes = new Uint8Array(size).fill(3);
  const hash = await sha256(bytes);
  const id = 't';
  const done = waitForState(receiver, 'completed');
  pair.a._pair.b._deliver({ data: encodeMeta({ id, name: 'f', size, chunks: 1, hash }) });
  pair.a._pair.b._deliver({ data: packChunk(0, bytes) });
  pair.a._pair.b._deliver({ data: encodeEnd({ id, hash }) });
  await done;

  // 完成后再来垃圾消息 / 旧 end
  pair.a._pair.b._deliver({ data: 'not-json' });
  pair.a._pair.b._deliver({ data: packChunk(0, new Uint8Array(CHUNK_SIZE)) });
  pair.a._pair.b._deliver({ data: encodeEnd({ id, hash }) });
  await settle(5);
  assert.equal(receiver.state, 'completed');
  pair.drop();
});

test('id 不匹配的 end 不能完成当前传输', async () => {
  const pair = new FakePair({ bytesPerTick: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b);
  const size = CHUNK_SIZE;
  const bytes = new Uint8Array(size).fill(4);
  const hash = await sha256(bytes);
  pair.a._pair.b._deliver({ data: encodeMeta({ id: 'a', name: 'f', size, chunks: 1, hash }) });
  pair.a._pair.b._deliver({ data: packChunk(0, bytes) });
  pair.a._pair.b._deliver({ data: encodeEnd({ id: 'other', hash }) });
  await settle(10);
  assert.equal(receiver.state, 'active', '异 id 的 end 被忽略，仍等待真正的 end');
  const done = waitForState(receiver, 'completed');
  pair.a._pair.b._deliver({ data: encodeEnd({ id: 'a', hash }) });
  await done;
  pair.drop();
});

test('越界序号的迟到块被忽略；范围内但大小错误的块判失败', async () => {
  const pair = new FakePair({ bytesPerTick: CHUNK_SIZE });
  const receiver = new FileReceiver(pair.b);
  const id = 'x';
  const chunkBytes = new Uint8Array(CHUNK_SIZE);
  const hash = await sha256(chunkBytes);
  pair.a.send(encodeMeta({ id, name: 'f', size: CHUNK_SIZE, chunks: 1, hash }));
  pair.tick();
  assert.equal(receiver.state, 'active');
  // 越界块 #5：当作旧传输迟到数据忽略，不失败、不完成
  pair.a.send(packChunk(5, new Uint8Array(CHUNK_SIZE)));
  pair.tick();
  pair.tick();
  await settle(5);
  assert.equal(receiver.state, 'active');

  // 范围内但大小错误（截短块）：必须失败
  const fail = waitForState(receiver, 'failed');
  pair.a.send(packChunk(0, new Uint8Array(CHUNK_SIZE - 1)));
  pair.tick();
  pair.tick();
  const detail = await fail;
  assert.match(detail.reason, /大小不符/);
  pair.drop();
});


// ---------- 显式恢复 ----------

test('断线后显式恢复：新连接只发下一块起的数据，整文件逐字节一致', async () => {
  const size = CHUNK_SIZE * 6 + 77;
  const bytes = new Uint8Array(size);
  for (let i = 0; i < size; i++) bytes[i] = (i * 13 + 5) & 0xff;
  const file = new Blob([bytes]);
  file.name = 'resume.bin';
  const fullHash = await sha256(bytes);

  const pair1 = new FakePair({ bytesPerTick: CHUNK_SIZE });
  let sender1 = new FileSender(pair1.a, { perChunkDelay: 2 });
  let receiver1 = new FileReceiver(pair1.b);
  const start = sender1.start(file);
  await waitForState(sender1, 'active');
  await waitForState(receiver1, 'active');
  for (let waited = 0; waited < 500 && receiver1.receivedBytes < 3 * CHUNK_SIZE; waited += 2) {
    await settle(2);
  }
  assert.ok(receiver1.receivedBytes >= 3 * CHUNK_SIZE);
  pair1.drop();
  await start.catch(() => {});
  await settle(5);
  assert.equal(sender1.state, 'interrupted');
  assert.equal(receiver1.state, 'interrupted');

  const resumeSnapshot = await receiver1.suspendForReconnect();
  sender1.suspendForReconnect();
  assert.ok(resumeSnapshot.nextChunk >= 3);
  assert.equal(resumeSnapshot.id, sender1.id);
  assert.equal(resumeSnapshot.prefixBytes, resumeSnapshot.nextChunk * CHUNK_SIZE);
  assert.equal(resumeSnapshot.prefixHash,
    await sha256(bytes.slice(0, resumeSnapshot.prefixBytes)));

  const pair2 = new FakePair({ bytesPerTick: CHUNK_SIZE });
  const attemptA = 'attempt-A';
  const attemptB = 'attempt-B';
  let newChunkMessages = 0;
  const originalSend = pair2.a.send.bind(pair2.a);
  pair2.a.send = (data) => {
    if (typeof data !== 'string') newChunkMessages += 1;
    return originalSend(data);
  };
  const sender2 = new FileSender(pair2.a, {
    attemptId: attemptA,
    getResumeFile: () => file,
  });
  const receiver2 = new FileReceiver(pair2.b, {
    attemptId: attemptB,
    resume: resumeSnapshot,
  });
  assert.equal(receiver2.attemptId, attemptB);
  assert.equal(sender2.attemptId, attemptA);
  assert.equal(receiver2.state, 'resumable');
  assert.equal(receiver2.nextChunk, resumeSnapshot.nextChunk);

  const completed = waitForState(receiver2, 'completed');
  receiver2.resume();
  const result = await completed;
  await waitForState(sender2, 'completed');

  assert.equal(newChunkMessages, 7 - resumeSnapshot.nextChunk, '只发送剩余块');
  assert.equal(result.size, size);
  assert.equal(result.hash, fullHash);
  const got = await readBlob(result.blob);
  assert.ok(got.every((b, i) => b === bytes[i]), '恢复后整文件逐字节一致');
  pair2.drop();
});

test('前缀摘要不符：发送方拒绝恢复，双方失败并清空半成品', async () => {
  const size = CHUNK_SIZE * 3;
  const bytes = new Uint8Array(size).fill(20);
  const file = new Blob([bytes]);
  file.name = 'prefix.bin';
  const snapshot = new ReceiverResume({
    id: 'p',
    name: 'prefix.bin',
    size,
    totalChunks: 3,
    hash: await sha256(bytes),
    nextChunk: 1,
    prefixHash: 'f'.repeat(64),
    savedChunks: [new Uint8Array(CHUNK_SIZE).fill(20)],
  });
  const pair = new FakePair();
  const sender = new FileSender(pair.a, { getResumeFile: () => file });
  const receiver = new FileReceiver(pair.b, { resume: snapshot });
  const receiverFailed = waitForState(receiver, 'failed');
  receiver.resume();
  const detail = await receiverFailed;
  await waitForState(sender, 'failed');
  assert.match(detail.reason, /前缀/);
  assert.equal(receiver.receivedBytes, 0);
  pair.drop();
});

test('换文件：整文件身份或长度不符时拒绝恢复且不发送任何块', async () => {
  const original = new Uint8Array(CHUNK_SIZE * 2).fill(1);
  const changed = new Uint8Array(original.byteLength + 1);
  changed.set(original);
  changed[changed.length - 1] = 2;
  const file = new Blob([changed]);
  file.name = 'same-name.bin';
  const snapshot = new ReceiverResume({
    id: 'changed',
    name: 'same-name.bin',
    size: original.length,
    totalChunks: 2,
    hash: await sha256(original),
    nextChunk: 1,
    prefixHash: await sha256(original.slice(0, CHUNK_SIZE)),
    savedChunks: [original.slice(0, CHUNK_SIZE)],
  });
  const pair = new FakePair();
  let binarySends = 0;
  const raw = pair.a.send.bind(pair.a);
  pair.a.send = (d) => {
    if (typeof d !== 'string') binarySends += 1;
    return raw(d);
  };
  const sender = new FileSender(pair.a, { getResumeFile: () => file });
  const receiver = new FileReceiver(pair.b, { resume: snapshot });
  receiver.resume();
  await waitForState(receiver, 'failed');
  await waitForState(sender, 'failed');
  assert.equal(binarySends, 0);
  pair.drop();
});

test('恢复后取消：双方清空不可再信任的半成品，后续迟到块/end 被隔离', async () => {
  const size = CHUNK_SIZE * 4;
  const bytes = new Uint8Array(size).fill(40);
  const file = new Blob([bytes]);
  file.name = 'cancel-resume.bin';
  const snapshot = new ReceiverResume({
    id: 'cr',
    name: 'cancel-resume.bin',
    size,
    totalChunks: 4,
    hash: await sha256(bytes),
    nextChunk: 2,
    prefixHash: await sha256(bytes.slice(0, CHUNK_SIZE * 2)),
    savedChunks: [
      bytes.slice(0, CHUNK_SIZE),
      bytes.slice(CHUNK_SIZE, CHUNK_SIZE * 2),
    ],
  });
  const pair = new FakePair({ bytesPerTick: CHUNK_SIZE });
  const sender = new FileSender(pair.a, { perChunkDelay: 5, getResumeFile: () => file });
  const receiver = new FileReceiver(pair.b, { resume: snapshot });
  receiver.resume();
  await waitForState(sender, 'active');
  await settle(2);
  receiver.cancel('恢复后仍不要了');
  await waitForState(receiver, 'canceled');
  await waitForState(sender, 'canceled');
  assert.equal(receiver.receivedBytes, 0);

  // 模拟取消后迟到的剩余块和 end：不能把新尝试/已取消状态变成完成。
  pair.b._deliver({ data: packChunk(2, bytes.slice(2 * CHUNK_SIZE, 3 * CHUNK_SIZE)) });
  pair.b._deliver({ data: encodeEnd({ id: 'cr', hash: await sha256(bytes) }) });
  pair.a._deliver({ data: encodeReceipt({ id: 'cr', ok: true, hash: await sha256(bytes) }) });
  await settle(5);
  assert.equal(receiver.state, 'canceled');
  assert.equal(sender.state, 'canceled');
  pair.drop();
});

test('恢复前用户放弃前缀：快照被清空，随后新 meta 可正常首次传输', async () => {
  const size = CHUNK_SIZE * 2;
  const bytes = new Uint8Array(size).fill(31);
  const snapshot = new ReceiverResume({
    id: 'discard-before-resume',
    name: 'old.bin',
    size,
    totalChunks: 2,
    hash: await sha256(bytes),
    nextChunk: 1,
    prefixHash: await sha256(bytes.slice(0, CHUNK_SIZE)),
    savedChunks: [bytes.slice(0, CHUNK_SIZE)],
  });
  const pair = new FakePair();
  const receiver = new FileReceiver(pair.b, { resume: snapshot });
  assert.equal(receiver.receivedBytes, CHUNK_SIZE);
  receiver.cancel('用户放弃');
  assert.equal(receiver.state, 'canceled');
  assert.equal(receiver.receivedBytes, 0);

  const fresh = new Uint8Array(CHUNK_SIZE + 2).fill(62);
  const freshHash = await sha256(fresh);
  const done = waitForState(receiver, 'completed');
  pair.a.send(encodeMeta({
    id: 'fresh', name: 'fresh.bin', size: fresh.length,
    chunks: 2, hash: freshHash,
  }));
  pair.a.send(packChunk(0, fresh.slice(0, CHUNK_SIZE)));
  pair.a.send(packChunk(1, fresh.slice(CHUNK_SIZE)));
  pair.a.send(encodeEnd({ id: 'fresh', hash: freshHash }));
  const result = await done;
  assert.equal(result.name, 'fresh.bin');
  pair.drop();
});

test('恢复后最终整文件 SHA-256 不符：失败回执清空接收前缀并使发送方失败', async () => {
  const original = new Uint8Array(CHUNK_SIZE * 3).fill(50);
  const file = new Blob([original]);
  file.name = 'tail.bin';
  const snapshot = new ReceiverResume({
    id: 'hash-after-resume',
    name: 'tail.bin',
    size: original.length,
    totalChunks: 3,
    hash: await sha256(original),
    nextChunk: 1,
    prefixHash: await sha256(original.slice(0, CHUNK_SIZE)),
    savedChunks: [original.slice(0, CHUNK_SIZE)],
  });
  const pair = new FakePair();
  pair.setTransform((from, message) => {
    if (from !== pair.a || message.kind !== 'binary') return message;
    const seq = new DataView(message.data).getUint32(0, false);
    if (seq !== 2) return message;
    const copy = message.data.slice(0);
    new Uint8Array(copy)[copy.byteLength - 1] ^= 0xff;
    return { kind: 'binary', data: copy, size: copy.byteLength };
  });
  const sender = new FileSender(pair.a, { getResumeFile: () => file });
  const receiver = new FileReceiver(pair.b, { resume: snapshot });
  receiver.resume();
  const detail = await waitForState(receiver, 'failed');
  await waitForState(sender, 'failed');
  assert.match(detail.reason, /SHA-256/);
  assert.equal(receiver.receivedBytes, 0);
  pair.drop();
});

test('损坏的本地续传身份不能被新接收对象采纳', () => {
  assert.throws(() => new FileReceiver(new FakeDataChannel('bad-resume'), {
    resume: new ReceiverResume({
      id: 'bad', name: 'x', size: CHUNK_SIZE, totalChunks: 1,
      hash: 'a'.repeat(64), nextChunk: 1, prefixHash: 'b'.repeat(64),
      savedChunks: [],
    }),
  }), /连续块/);
});
