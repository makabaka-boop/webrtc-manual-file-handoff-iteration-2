# WebRTC 手工连接 · 文件直传

两个浏览器页面通过**人工复制连接信息（SDP）**建立 WebRTC DataChannel，点对点传输
**一个不超过 5 MiB 的文件**。没有信令服务器、没有应用服务器、文件不经过任何中间机器。

> 两个页面可以是同一台机器上的两个标签页，也可以分处两地 —— 连接信息通过你信任的
> 任意带外渠道（聊天、U 盘、纸条……）复制即可。仅配置了一个公共 STUN 用于 NAT 打洞，
> STUN 只协助发现地址，不转发文件内容。

## 使用

任选一种方式打开页面（需通过 http 提供，`file://` 下 ES Module 可能受限）：

```bash
npm start                 # http://127.0.0.1:8080
# 或任意静态服务器，例如： python3 -m http.server 8080
```

1. 页面 A 选择「发起方」→ 点 **生成连接信息** → 复制文本框内容发给页面 B。
2. 页面 B 选择「应答方」→ 粘贴连接信息 → 点 **生成应答信息** → 复制应答文本发回 A。
3. 页面 A 粘贴应答 → 点 **接受应答**，两端状态都变为「已连接」。
4. A 选择文件（≤ 5 MiB）点 **发送**；B 在进度到 100% 且 SHA-256 校验通过后，
   才会出现 **下载** 链接，并向 A 回传成功回执；A 只有收到该回执后才显示传输成功。
   任一方都可在传输中点取消。
5. 若 DataChannel 在末尾断开，双方页面保持打开并点 **重新连接**，重新交换一次
   offer/answer。B 点 **恢复本次传输**：B 只凭当前页面内存中的连续块前缀声明原
   传输 id、下一块序号和前缀 SHA-256；A 核对当前所选文件的名称、总长、整文件
   SHA-256 与该前缀字节，全部匹配后才从下一块续发。

无法连接（错误 SDP、ICE 失败、对端长时间无响应）或传输失败（通道断开、坏块、哈希不符）
都会**明确显示失败原因**，不会出现“看着成功了其实没传完”的情况。

## 协议

控制消息走 DataChannel 文本通道（JSON），文件数据走二进制通道，同一通道严格有序：

| 消息 | 方向 | 内容 |
| --- | --- | ---|
| `meta` | 发 → 收 | `{ kind:'meta', id, name, size, chunks, hash }` 首次传输开始 |
| `resume` | 收 → 发 | `{ kind:'resume', id, name, size, chunks, hash, nextChunk, prefixHash }` 请求续传 |
| `resume-accept` | 发 → 收 | `{ kind:'resume-accept', id, nextChunk, prefixHash }` 前缀核对通过 |
| `resume-reject` | 发 → 收 | `{ kind:'resume-reject', id, reason }` 文件/前缀不匹配 |
| 数据块 | 发 → 收 | 二进制：4 字节大端序号 + 载荷（载荷 ≤ 16 KiB）|
| `end` | 发 → 收 | `{ kind:'end', id, hash }` 结束 |
| `receipt` | 收 → 发 | `{ kind:'receipt', id, ok, hash?, reason? }` 校验回执 |
| `cancel` | 双向 | `{ kind:'cancel', id }` 取消 |

- 块大小 `16 KiB`，单文件上限 `5 MiB`（见 `src/protocol.js`）。
- 序号从 0 连续递增；接收方校验：块序号完整、每块大小与位置自洽、总字节数等于 `size`、
  拼接后 SHA-256 等于 `meta.hash`（`end.hash` 也必须一致）。**任何一项不符都判失败**。
- 接收方仅在全部校验通过后生成下载链接，并回传 `{ ok:true, hash }`；失败则回传
  `{ ok:false, reason }`。发送方收到成功且哈希匹配的回执后才进入完成状态；收不到回执会超时失败，
  因此不会出现“接收方已失败、发送方却报告成功”。
- 续传只使用当前页面内存，不引入服务器存储。接收方只暂存从 0 开始、每个块边界完整的连续前缀；
  任何带洞或乱序数据都不能作为前缀。`hash` 是原整文件 SHA-256，`prefixHash` 是该前缀字节的
  SHA-256，`nextChunk` 是下一块序号。发送方重新计算整文件哈希并实际读取文件前缀比对，
  匹配才发送 `resume-accept` 并从 `nextChunk` 开始 pump；换文件、改名、长度变化、前缀不符均回
  `resume-reject`，双方失败并丢弃半成品。

### 背压

发送方（`FileSender`）在每次 `send()` 后检查 `bufferedAmount`，达到高水位
（`bufferedAmountLowThreshold` 的 4 倍，默认阈值 64 KiB）即暂停读取文件并等待
`bufferedamountlow` 事件，不会一次性把文件塞进发送缓冲；剩余块留在文件中，不占内存。

### 取消、断线、重连与“迟到事件”

每个逻辑传输有稳定随机 `id`；每次新连接还会生成独立 `attemptId`，并创建新的
FileSender/FileReceiver 对象。收发双方各自维护内部 `generation` 代际令牌：

- 主动取消 / 校验失败 / 新 `meta` 都会推进代际并**丢弃半成品**；
- 普通断线不伪装成功，也不立即销毁前缀：发送方进入 `interrupted`，接收方保留连续前缀进入可恢复状态；
- 用户显式点重新连接并交换新 SDP 后，接收方必须再点 **恢复本次传输**，通过身份与前缀核对才续发；
- 进行中的读块、等缓冲、异步 SHA-256 回来时先比对代际，过期结果一律作废；
- 旧传输/旧连接迟到的块、`end`、成功回执、`cancel` 按 attempt 对象、id、序号和状态隔离，
  **不能完成或打坏新尝试**。

## 代码结构

```
index.html              页面
src/protocol.js         常量、消息编解码、块打包、SHA-256
src/transfer.js         FileSender（背压/取消）与 FileReceiver（校验/丢弃）
src/signaling.js        手工 offer/answer（非 trickle，一段文本即完整 SDP）
src/fake-channel.js     可替换 DataChannel 的测试替身（背压/断开/篡改）
tests/unit/             node:test 单元测试（协议 + 全部传输竞争场景）
tests/page/             Playwright 双页面真实 WebRTC 端到端测试
```

## 测试

```bash
npm test                 # 单元测试（无需浏览器）
npm run test:e2e         # 页面测试：两个真实页面走完整连接+传输流程
npm start                # 手动体验
```

单元测试（`tests/unit/transfer.test.js`）覆盖：

- 正常传输（含空文件、非块对齐文件）逐字节一致；超 5 MiB 直接失败且不写通道；
- **缓冲回落**：慢链路 + 低高水位下反复暂停/恢复，缓冲不无界增长；
- **断开/恢复**：传输中途通道关闭不会伪装完成；显式重连后核对续传身份与前缀摘要，只发送剩余块；
- **坏块**：块头损坏、块大小不符、内容被改导致哈希不符，均失败且不提供下载；
- **取消竞争**：背压等待中取消不挂起、双方丢弃半成品；接收方拒收会让发送方停下；
- **重连/迟到事件**：新 `meta` 清空旧块，新连接使用独立 attempt，旧 id 的迟到块/`end`/成功回执/`cancel` 不影响新传输。

页面测试（`tests/page/transfer.page.test.js`）打开两个 Chromium 页面，人工式复制
offer/answer 建立真实 DataChannel，发送一个多块文件并在接收页面触发真实下载，
独立复算大小与 SHA-256；另含传输中取消、可控断线后真实双页二次握手续传、取消清理和
旧尝试迟到事件隔离用例。查询参数 `?ice=host` 只用本地 host 候选，
`?chunkDelay=ms` 为发送每块增加延迟以制造可取消/断线窗口（仅测试用）。

> 在没有 root 的环境里，页面测试需要的 Chromium 动态库可解压到用户目录，
> 由 `tests/page/browser-env.js` 通过 `LD_LIBRARY_PATH` 注入。
