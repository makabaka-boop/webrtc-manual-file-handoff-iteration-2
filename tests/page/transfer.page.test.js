// 页面级测试：两个真实浏览器页面走一次完整的“手工复制 SDP → 建立 DataChannel
// → 发送文件 → 校验 → 下载”流程。文件内容与 SHA-256 在测试进程内独立校验。
import { test, expect, chromium } from '@playwright/test';
import { createHash } from 'node:crypto';
import { startServer } from './static-server.js';
import { launchEnv, launchArgs } from './browser-env.js';

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

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

  // A 作为发起方生成连接信息
  await pageA.click('#offer-btn');
  await expect(pageA.locator('#signal-out')).not.toHaveValue('');
  const offer = await pageA.inputValue('#signal-out');

  // B 选择应答方角色，粘贴 offer，生成应答
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

  return { browser, pageA, pageB };
}

async function reconnectWithManualSignaling(pageA, pageB) {
  await Promise.all([pageA.click('#restart'), pageB.click('#restart')]);
  await expect(pageA.locator('#conn-state')).toHaveText(/未连接/);
  await expect(pageB.locator('#conn-state')).toHaveText(/未连接/);
  await expect(pageA.locator('#offer-btn')).toBeEnabled();
  await expect(pageB.locator('#answer-btn')).toBeEnabled();

  await pageA.click('#offer-btn');
  await expect(pageA.locator('#signal-out')).not.toHaveValue('');
  const offer = await pageA.inputValue('#signal-out');

  await pageB.fill('#signal-in', offer);
  await pageB.click('#answer-btn');
  await expect(pageB.locator('#signal-out')).not.toHaveValue('');
  const answer = await pageB.inputValue('#signal-out');

  await pageA.fill('#signal-in', answer);
  await expect(pageA.locator('#accept-btn')).toBeEnabled();
  await pageA.click('#accept-btn');
  await expect(pageA.locator('#conn-state')).toHaveText(/已连接/);
  await expect(pageB.locator('#conn-state')).toHaveText(/已连接/);
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


test('双端页面：可控断线后交换新连接信息，只续传剩余字节并隔离旧尝试迟到事件', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;
  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=100');

  async function closeTransport(page) {
    await page.evaluate(() => {
      window.__app.session.channel.close();
      window.__app.session.pc.close();
    });
  }

  try {
    const size = 200_000; // 13 个块（12 * 16384 + 3392）
    const bytes = patternBytes(size);
    const expectedHash = createHash('sha256').update(bytes).digest('hex');
    await pageA.setInputFiles('#file-input', {
      name: 'resume-real.bin',
      mimeType: 'application/octet-stream',
      buffer: bytes,
    });
    await pageA.click('#send-btn');
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中/);

    // 稳定制造部分前缀：进度落在 40%~75% 后手动断开底层 PC/DataChannel。
    await expect.poll(async () => {
      const text = await pageB.locator('#progress-text').textContent();
      const match = text.match(/([0-9.]+)%/);
      return match ? Number(match[1]) : 0;
    }, { timeout: 30_000 }).toBeGreaterThanOrEqual(20);
    const prefixInfo = await pageB.evaluate(() => {
      const r = window.__app.session.receiver;
      return {
        nextChunk: r.nextChunk,
        receivedBytes: r.receivedBytes,
        id: r.info.id,
        hash: r.info.hash,
      };
    });
    expect(prefixInfo.nextChunk).toBeGreaterThan(0);
    expect(prefixInfo.receivedBytes).toBeLessThan(size);

    await Promise.all([closeTransport(pageA), closeTransport(pageB)]);
    await expect(pageA.locator('#xfer-state')).toHaveText(/可恢复|连接中断/);
    await expect(pageB.locator('#xfer-state')).toHaveText(/可恢复|连接中断/);

    await reconnectWithManualSignaling(pageA, pageB);
    await expect(pageB.locator('#resume-recv')).toBeVisible();

    // 新连接是独立尝试；旧尝试仍保留中断状态，等待迟到事件隔离验证。
    const attempts = await pageB.evaluate(() => ({
      old: window.__app.session.detachedAttempts.at(-1)?.attemptId,
      current: window.__app.session.attemptId,
      oldState: window.__app.session.detachedAttempts.at(-1)?.receiver?.state,
      prefixBytes: window.__app.session.resume?.prefixBytes ?? null,
      nextChunk: window.__app.session.resume?.nextChunk ?? null,
    }));
    expect(attempts.old).toBeTruthy();
    expect(attempts.current).toBeTruthy();
    expect(attempts.current).not.toBe(attempts.old);
    expect(attempts.oldState).toBe('interrupted');
    expect(attempts.nextChunk).toBeGreaterThanOrEqual(prefixInfo.nextChunk);
    expect(attempts.nextChunk).toBeLessThan(13);
    expect(attempts.prefixBytes).toBe(attempts.nextChunk * 16 * 1024);

    await pageB.evaluate((nextChunk) => {
      window.__app.sendStaleChunkToOldReceiver(
        nextChunk,
        new Uint8Array(16 * 1024).fill(0xff),
      );
      window.__app.sendStaleEndToOldReceiver('00'.repeat(32));
    }, attempts.nextChunk);
    await pageA.evaluate(() => window.__app.sendStaleReceiptToOldSender('00'.repeat(32)));
    await sleep(100);
    const staleStates = await Promise.all([
      pageB.evaluate(() => window.__app.session.detachedAttempts.at(-1).receiver.state),
      pageA.evaluate(() => window.__app.session.detachedAttempts.at(-1).sender.state),
    ]);
    expect(staleStates).toEqual(['interrupted', 'interrupted']);

    const countAfterResume = await pageA.evaluate(async () => {
      const ch = window.__app.session.channel;
      let binaryBytes = 0;
      const original = ch.send.bind(ch);
      ch.send = (data) => {
        if (typeof data !== 'string') binaryBytes += data.byteLength;
        return original(data);
      };
      window.__app.countBinaryBytes = () => binaryBytes;
      await Promise.resolve();
    });
    void countAfterResume;
    await pageB.click('#resume-recv');
    await expect(pageB.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });
    await expect(pageA.locator('#xfer-state')).toHaveText(/完成/, { timeout: 30_000 });

    const resumedBinaryBytes = await pageA.evaluate(() => window.__app.countBinaryBytes());
    const resumedPayloadBytes = size - attempts.prefixBytes;
    const resumedChunkCount = 13 - attempts.nextChunk;
    expect(resumedBinaryBytes).toBe(resumedPayloadBytes + resumedChunkCount * 4);
    expect(resumedBinaryBytes).toBeLessThan(size);

    const link = pageB.locator('#download-box a.download');
    await expect(link).toBeVisible();
    await expect(pageB.locator('#hash-box')).toContainText(expectedHash);
    const [download] = await Promise.all([
      pageB.waitForEvent('download'),
      link.click(),
    ]);
    const stream = await download.createReadStream();
    const chunks = [];
    for await (const c of stream) chunks.push(c);
    const got = Buffer.concat(chunks);
    expect(got.length).toBe(size);
    expect(got.equals(bytes)).toBe(true);
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
});

test('双端页面：恢复后取消会清理前缀，旧迟到事件仍不能完成', async () => {
  const { server } = await startServer(0);
  const baseURL = `http://127.0.0.1:${server.address().port}`;
  const { browser, pageA, pageB } = await connectTwoPages(baseURL, '/?ice=host&chunkDelay=100');

  try {
    const bytes = patternBytes(180_000);
    await pageA.setInputFiles('#file-input', {
      name: 'cancel-after-resume.bin',
      mimeType: 'application/octet-stream',
      buffer: bytes,
    });
    await pageA.click('#send-btn');
    await expect(pageB.locator('#xfer-state')).toHaveText(/传输中/);
    await expect.poll(async () => {
      const text = await pageB.locator('#progress-text').textContent();
      return Number(text.match(/([0-9.]+)%/)?.[1] ?? 0);
    }).toBeGreaterThanOrEqual(35);
    const prefixInfo = await pageB.evaluate(() => ({
      nextChunk: window.__app.session.receiver.nextChunk,
      receivedBytes: window.__app.session.receiver.receivedBytes,
    }));
    await Promise.all([
      pageA.evaluate(() => { window.__app.session.channel.close(); window.__app.session.pc.close(); }),
      pageB.evaluate(() => { window.__app.session.channel.close(); window.__app.session.pc.close(); }),
    ]);
    await reconnectWithManualSignaling(pageA, pageB);
    await pageB.click('#resume-recv');
    await expect(pageA.locator('#cancel-send')).toBeVisible();
    await pageA.click('#cancel-send');
    await expect(pageA.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/, { timeout: 15_000 });
    expect(await pageB.evaluate(() => window.__app.session.receiver?.receivedBytes ?? 0)).toBe(0);

    await pageB.evaluate((nextChunk) => {
      window.__app.sendStaleChunkToOldReceiver(nextChunk, new Uint8Array(16 * 1024));
      window.__app.sendStaleEndToOldReceiver('11'.repeat(32));
    }, prefixInfo.nextChunk);
    await sleep(100);
    await expect(pageB.locator('#download-box')).toBeHidden();
    await expect(pageB.locator('#xfer-state')).toHaveText(/已取消/);
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
