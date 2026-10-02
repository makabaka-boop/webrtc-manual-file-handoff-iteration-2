
test('双端页面：真实断线与新 SDP 重连后显式续传，旧通道迟到 end/receipt 被隔离', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;

  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=20');

  try {
    const size = 300_000;
    const bytes = patternBytes(size);
    const expectedHash = createHash('sha256').update(bytes).digest('hex');

    await pageA.setInputFiles('#file-input', { name: 'resume.bin', mimeType: 'application/octet-stream', buffer: bytes });
    await pageA.click('#send-btn');
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中/);

    // 等到至少 3 个完整块到达，但远未传完。
    await expect.poll(async () => {
      return pageB.evaluate(() => window.__app.session.receiver.receivedBytes);
    }, { timeout: 15_000, intervals: [20, 20] }).toBeGreaterThanOrEqual(3 * 16_384);

    await pageA.evaluate(() => { window.__oldChannel = window.__app.session.channel; });
    await pageB.evaluate(() => { window.__oldChannel = window.__app.session.channel; });

    // 可控断开：两端关闭当前 DataChannel，页面仍保持打开与内存前缀。
    await pageA.evaluate(() => window.__app.disconnectDataChannel());
    await pageB.evaluate(() => window.__app.disconnectDataChannel());
    await expect(pageA.locator('#xfer-state')).toHaveText(/等待恢复/);
    await expect(pageB.locator('#xfer-state')).toHaveText(/等待恢复/);

    const prefix = await pageB.evaluate(() => ({
      nextChunk: window.__app.session.receiver.nextChunk,
      receivedBytes: window.__app.session.receiver.receivedBytes,
    }));
    expect(prefix.receivedBytes).toBe(prefix.nextChunk * 16_384);
    expect(prefix.nextChunk).toBeGreaterThan(0);
    expect(prefix.nextChunk).toBeLessThan(Math.ceil(size / 16_384));

    // 重新交换一套 offer/answer；传输对象保留并 attach 新 DataChannel。
    await reconnectTwoPages(pageA, pageB);
    await expect(pageA.locator('#resume-send')).toBeVisible();
    await expect(pageB.locator('#resume-recv')).toBeVisible();

    // 记录新连接上实际发送的二进制块序号。
    await pageA.evaluate(() => {
      const ch = window.__app.session.channel;
      const originalSend = ch.send.bind(ch);
      window.__resumeSequences = [];
      ch.send = (data) => {
        if (data instanceof ArrayBuffer) {
          window.__resumeSequences.push(new DataView(data).getUint32(0, false));
        }
        return originalSend(data);
      };
    });

    // 必须由接收方先显式声明恢复身份/前缀，再由发送方核对同一文件后续传。
    await pageB.click('#resume-recv');
    await pageA.click('#resume-send');
    await expect(pageB.locator('#recv-meta')).toContainText(`从块 #${prefix.nextChunk} 续传`);
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中/);

    // 新尝试已开始但尚未完成时，重放旧 DataChannel 的迟到 end 与成功回执。
    const transferId = await pageA.evaluate(() => window.__app.session.sender.id);
    await pageB.evaluate(({ id, hash }) => {
      const event = new Event('message');
      event.data = JSON.stringify({ kind: 'end', id, hash });
      window.__oldChannel.dispatchEvent(event);
    }, { id: transferId, hash: expectedHash });
    await pageA.evaluate(({ id, hash }) => {
      const event = new Event('message');
      event.data = JSON.stringify({ kind: 'receipt', id, ok: true, hash });
      window.__oldChannel.dispatchEvent(event);
    }, { id: transferId, hash: expectedHash });
    await expect(pageA.locator('#xfer-state')).toHaveText(/传输中|等待接收校验/);
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中|校验中/);

    await expect(pageA.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });
    const sequences = await pageA.evaluate(() => window.__resumeSequences);
    expect(sequences[0]).toBe(prefix.nextChunk);
    expect(sequences).toHaveLength(Math.ceil(size / 16_384) - prefix.nextChunk);

    const downloadLink = pageB.locator('#download-box a.download');
    await expect(downloadLink).toBeVisible();
    const [download] = await Promise.all([
      pageB.waitForEvent('download', { timeout: 10_000 }),
      downloadLink.click(),
    ]);
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    const got = Buffer.concat(chunks);
    expect(got.length).toBe(size);
    expect(got.equals(bytes)).toBe(true);
    expect(createHash('sha256').update(got).digest('hex')).toBe(expectedHash);
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});

test('双端页面：断线重连后的恢复被取消时，双方清空半成品且不再提供下载', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;

  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=20');

  try {
    const bytes = patternBytes(200_000);
    await pageA.setInputFiles('#file-input', { name: 'cancel-resume.bin', mimeType: 'application/octet-stream', buffer: bytes });
    await pageA.click('#send-btn');
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中/);

    await expect.poll(async () => {
      return pageB.evaluate(() => window.__app.session.receiver.receivedBytes);
    }, { timeout: 15_000, intervals: [20, 20] }).toBeGreaterThanOrEqual(2 * 16_384);

    await pageA.evaluate(() => window.__app.disconnectDataChannel());
    await pageB.evaluate(() => window.__app.disconnectDataChannel());
    await expect(pageA.locator('#xfer-state')).toHaveText(/等待恢复/);
    await expect(pageB.locator('#xfer-state')).toHaveText(/等待恢复/);
    await reconnectTwoPages(pageA, pageB);

    await pageB.click('#cancel-recv');
    await expect(pageA.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });
    expect(await pageB.evaluate(() => window.__app.session.receiver.receivedBytes)).toBe(0);
    expect(await pageB.evaluate(() => window.__app.session.receiver.nextChunk)).toBe(0);
    await expect(pageB.locator('#download-box')).toBeHidden();
    await expect(pageB.locator('#resume-recv')).toBeHidden();
    await expect(pageA.locator('#resume-send')).toBeHidden();
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});

// 页面级测试：两个真实浏览器页面走一次完整的“手工复制 SDP → 建立 DataChannel
// → 发送文件 → 校验 → 下载”流程。文件内容与 SHA-256 在测试进程内独立校验。
import { test, expect, chromium } from '@playwright/test';
import { createHash } from 'node:crypto';
import { startServer } from './static-server.js';
import { launchEnv, launchArgs } from './browser-env.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

async function establishConnection(pageA, pageB) {
  // A 作为发起方生成连接信息
  await pageA.click('#offer-btn');
  await expect(pageA.locator('#signal-out')).not.toHaveValue('');
  const offer = await pageA.inputValue('#signal-out');

  // B 作为应答方，粘贴 offer，生成应答
  await pageB.check('input[name="role"][value="answer"]');
  await pageB.fill('#signal-in', offer);
  await pageB.click('#answer-btn');
  await expect(pageB.locator('#signal-out')).not.toHaveValue('');
  const answer = await pageB.inputValue('#signal-out');

  // A 粘贴 answer 并接受
  await pageA.fill('#signal-in', answer);
  await pageA.click('#accept-btn');

  // 两端都应明确显示已连接，且传输面板出现
  await expect(pageA.locator('#conn-state')).toHaveText(/已连接/);
  await expect(pageB.locator('#conn-state')).toHaveText(/已连接/);
  await expect(pageA.locator('#transfer-panel')).toBeVisible();
  await expect(pageB.locator('#transfer-panel')).toBeVisible();
}

async function connectTwoPages(baseURL, pageQuery = '/?ice=host') {
  const browser = await chromium.launch({
    headless: true,
    env: launchEnv(),
    args: launchArgs,
  });
  const pageA = await browser.newPage();
  const pageB = await browser.newPage();

  // 同机测试：只用 host 候选，跳过公网 STUN。
  await pageA.goto(`${baseURL}${pageQuery}`);
  await pageB.goto(`${baseURL}${pageQuery}`);
  await establishConnection(pageA, pageB);

  return { browser, pageA, pageB };
}

async function reconnectTwoPages(pageA, pageB) {
  await pageA.evaluate(() => window.__app.reconnect());
  await pageB.evaluate(() => window.__app.reconnect());
  await establishConnection(pageA, pageB);
}

function patternBytes(size) {
  const buf = Buffer.alloc(size);
  for (let i = 0; i < size; i++) buf[i] = (i * 31 + 7) & 0xff;
  return buf;
}

test('双端页面：手工信令建立连接并完整传输、校验、下载一个文件', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;

  const { browser, pageA, pageB } = await connectTwoPages(baseURL);

  try {
    // ---- 先验证超限文件被明确拒绝，且对端无任何反应 ----
    const tooBig = Buffer.alloc(5 * 1024 * 1024 + 1, 0xab);
    await pageA.setInputFiles('#file-input', { name: 'too-big.bin', mimeType: 'application/octet-stream', buffer: tooBig });
    await pageA.click('#send-btn');
    await expect(pageA.locator('#xfer-state')).toHaveText(/失败/);
    await expect(pageA.locator('#xfer-error')).toContainText(/上限/);
    // 对端不应收到 meta
    await expect(pageB.locator('#xfer-state')).toHaveText(/空闲/);

    // ---- 正常文件：大小刻意非块对齐，覆盖多块 + 尾块 ----
    const size = 50_000;
    const bytes = patternBytes(size);
    const expectedHash = createHash('sha256').update(bytes).digest('hex');

    await pageA.setInputFiles('#file-input', { name: 'greeting.bin', mimeType: 'application/octet-stream', buffer: bytes });
    await pageA.click('#send-btn');

    // 发送方与接收方都显示完成（而非模糊的“已发送”）
    await expect(pageA.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });

    // 进度到 100%
    await expect(pageB.locator('#progress-text')).toContainText('100.0%');

    // 仅在哈希校验通过后才出现下载入口，且页面展示的哈希与独立计算一致
    const downloadLink = pageB.locator('#download-box a.download');
    await expect(downloadLink).toBeVisible();
    await expect(downloadLink).toContainText('greeting.bin');
    await expect(pageB.locator('#hash-box')).toContainText(expectedHash);

    // 主动点击下载并比对内容（Playwright 把 <a download> 转为 download 事件）
    const [download] = await Promise.all([
      pageB.waitForEvent('download', { timeout: 10_000 }),
      downloadLink.click(),
    ]);
    expect(download.suggestedFilename()).toBe('greeting.bin');
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    const got = Buffer.concat(chunks);
    expect(got.length).toBe(size);
    expect(got.equals(bytes)).toBe(true);
    expect(createHash('sha256').update(got).digest('hex')).toBe(expectedHash);
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});

test('双端页面：连续传输第二份时，第一份下载链接立即清除；取消后也不恢复', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;

  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=5');

  async function sendAndWaitDownload(name, bytes) {
    await pageA.setInputFiles('#file-input', { name, mimeType: 'application/octet-stream', buffer: bytes });
    await pageA.click('#send-btn');
    const link = pageB.locator('#download-box a.download');
    await expect(link).toBeVisible({ timeout: 30_000 });
    await expect(link).toContainText(name);
    await expect(pageA.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });
  }

  try {
    await sendAndWaitDownload('first.bin', patternBytes(20_000));

    // 第二份开始接收时，页面必须只呈现第二份，第一份 Blob URL 也要撤销。
    const second = patternBytes(300_000);
    await pageA.setInputFiles('#file-input', {
      name: 'second.bin',
      mimeType: 'application/octet-stream',
      buffer: second,
    });
    await pageA.click('#send-btn');
    await expect(pageB.locator('#recv-meta')).toContainText('second.bin');
    await expect(pageB.locator('#download-box')).toBeHidden();
    await expect(pageB.locator('#hash-box')).toBeHidden();

    await expect(pageB.locator('#cancel-recv')).toBeVisible();
    await pageB.click('#cancel-recv');
    await expect(pageA.locator('#xfer-state')).toHaveText(/已取消|失败/, { timeout: 15_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });

    await expect(pageB.locator('#download-box')).toBeHidden();
    await expect(pageB.locator('#hash-box')).toBeHidden();
    await sleep(200);
    await expect(pageB.locator('#download-box')).toBeHidden();
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});

test('双端页面：传输中取消，双方明确显示取消且接收方不提供下载', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;

  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=10');

  try {
    // 每块 10ms 延迟 + 512 KiB（32 块），制造稳定的“传输进行中”取消窗口
    const bytes = patternBytes(512 * 1024);
    await pageA.setInputFiles('#file-input', { name: 'cancel-me.bin', mimeType: 'application/octet-stream', buffer: bytes });
    await pageA.click('#send-btn');

    // 等接收方进入接收状态后立刻由发送方取消
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中|计算哈希中/);
    // 等到发送按钮区出现取消按钮再点
    await expect(pageA.locator('#cancel-send')).toBeVisible();
    await pageA.click('#cancel-send');

    await expect(pageA.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });

    // 关键：没有下载链接，进度被清零
    await expect(pageB.locator('#download-box')).toBeHidden();
    await expect(pageB.locator('#progress-text')).toHaveText('');
    await sleep(200);
    // 取消后迟到数据不会再完成传输
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/);
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});
