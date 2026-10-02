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

如果 DataChannel 在末尾前断开，两端页面不要刷新：点 **重置 / 重新连接**，像首次一样
重新交换一套 offer/answer。建立新连接后，先由接收方点 **恢复本次传输**（声明原传输 id、
文件名、总长、整文件哈希、下一块序号、前缀 SHA-256），再由发送方保持选择同一文件并点
**恢复本次传输**。发送方会重新核对文件身份并实际读取文件前缀计算 SHA-256；匹配后才从
下一块续发，不会重发已收部分。

无法连接（错误 SDP、ICE 失败、对端长时间无响应）或传输失败（通道断开、坏块、哈希不符）
都会**明确显示失败原因**，不会出现“看着成功了其实没传完”的情况。

## 协议

控制消息走 DataChannel 文本通道（JSON），文件数据走二进制通道，同一通道严格有序：

| 消息 | 方向 | 内容 |
| --- | --- | ---|
| `meta` | 发 → 收 | `{ kind:'meta', id, name, size, chunks, hash }` 首次开始 |
| `resume` | 收 → 发 | `{ kind:'resume', id, name, size, chunks, hash, nextChunk, prefixHash }` 显式请求恢复 |
| `resume-ack` | 发 → 收 | `{ kind:'resume-ack', id, ok, nextChunk?, prefixHash?, reason? }` 恢复核对结果 |
| 数据块 | 发 → 收 | 二进制：4 字节大端序号 + 载荷（载荷 ≤ 16 KiB）|
| `end` | 发 → 收 | `{ kind:'end', id, hash }` 结束 |
| `receipt` | 收 → 发 | `{ kind:'receipt', id, ok, hash?, reason? }` 校验回执 |
| `cancel` | 双向 | `{ kind:'cancel', id }` 取消 |

- 块大小 `16 KiB`，单文件上限 `5 MiB`（见 `src/protocol.js`）。
- 序号从 0 连续递增；接收方只暂存“从 0 开始、边界完整的连续块前缀”，空洞后的块不会被
  计入 `nextChunk`。接收方校验：块序号完整、每块大小与位置自洽、总字节数等于 `size`、
  拼接后 SHA-256 等于 `meta.hash`（`end.hash` 也必须一致）。**任何一项不符都判失败**。
- 恢复不是自动重发：接收方在新连接发送 `resume`，其中 `id/name/size/chunks/hash` 是
  原传输身份，`nextChunk` 是连续前缀后的下一块，`prefixHash` 是这些前缀字节的 SHA-256。
  发送方重新计算当前所选文件的整文件 SHA-256，核对身份和总长，并直接读取文件的前
  `nextChunk * CHUNK_SIZE` 字节复算摘要；全部匹配才发送 `resume-ack{ok:true}` 并从
  `nextChunk` 续发。换文件、前缀被污染、边界不一致都会收到 `ok:false`，双方清空半成品。
- 接收方仅在全部校验通过后生成下载链接，并回传 `{ ok:true, hash }`；失败则回传
  `{ ok:false, reason }`。发送方收到成功且哈希匹配的回执后才进入完成状态；收不到回执会超时失败，
  因此不会出现“接收方已失败、发送方却报告成功”。

### 背压

发送方（`FileSender`）在每次 `send()` 后检查 `bufferedAmount`，达到高水位
（`bufferedAmountLowThreshold` 的 4 倍，默认阈值 64 KiB）即暂停读取文件并等待
`bufferedamountlow` 事件，不会一次性把文件塞进发送缓冲；剩余块留在文件中，不占内存。

### 取消、关闭、重连与“迟到事件”

每次逻辑传输有随机 `id`；每次连接 / 首次尝试 / 恢复尝试还会推进独立的本地 `generation`
尝试代际：

- 普通断线只让传输进入 `interrupted`：发送方保留所选文件身份，接收方仅在当前页面内存中
  保留连续块前缀；没有服务器存储，刷新页面即丢失；
- “重置 / 重新连接”关闭旧 PeerConnection/DataChannel，再手工交换新 SDP；传输对象
  `attach` 新通道并推进代际、解绑旧通道；
- 用户取消 / 换文件 / 前缀不符 / 最终 SHA-256 不符都会进入终态并丢弃半成品；
- 进行中的读块、等缓冲、等恢复声明、异步 SHA-256 回来时先比对代际，过期结果一律作废；
- 旧通道迟到的数据块、`end`、成功回执、取消消息没有当前通道监听器，也不能靠相同 id
  完成新尝试；新连接上的新 `meta` 同样会清空旧前缀。

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
- **断开与显式恢复**：传输中途通道关闭后双方进入等待恢复；用新假通道/新真实页面重连，
  核对身份、边界和前缀摘要，只从下一块续发，完整文件逐字节一致；
- **恢复失败清理**：换文件、前缀字节被污染、恢复超时或用户取消，均明确失败/取消并清空；
- **坏块**：块头损坏、块大小不符、内容被改导致哈希不符，均失败且不提供下载；
- **取消竞争**：背压等待中取消不挂起、双方丢弃半成品；接收方拒收会让发送方停下；
- **重连/迟到事件**：新 `meta` 清空旧块，旧通道/旧 id 的迟到块、`end`、`cancel`、成功
  回执不影响新尝试。

页面测试（`tests/page/transfer.page.test.js`）打开两个 Chromium 页面，人工式复制
offer/answer 建立真实 DataChannel，发送一个多块文件并在接收页面触发真实下载，
独立复算大小与 SHA-256；另含可控断线后重新交换 SDP、显式续传、取消清理，以及旧通道
迟到 `end`/成功回执隔离用例。查询参数 `?ice=host` 只用本地 host 候选，
`?chunkDelay=ms` 为发送每块增加延迟以制造可取消窗口（仅测试用）。

> 在没有 root 的环境里，页面测试需要的 Chromium 动态库可解压到用户目录，
> 由 `tests/page/browser-env.js` 通过 `LD_LIBRARY_PATH` 注入。
