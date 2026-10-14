# SnowLuma 适配层（QQ/OneBot 发送侧）

> 背景：3020e 的 QQ→OneBot v11 桥从老 NapCat 切换到 **SnowLuma**。本文是 idol-bbq-utils
> 发送侧"SnowLuma 能力层"的运维/开发说明：失败语义判读表、媒体来源决策表、message_id
> 回执与撤回、健康探针、切换开关。能力调研依据见工作区
> `REPORT_snowluma_capabilities_2026-10-14.md`（SnowLuma v1.14.22 源码/文档核实）。
> **切换期两端都要能跑**：本文所有判读与投递路径都保留 NapCat 行为回退。

---

## 1. 失败语义判读表（`src/middleware/forwarder/onebot-semantics.ts`）

单点纯函数 `classifyOneBotResponse` / `classifyOneBotTransportError` /
`detectBridgeFlavor`，`qq.ts` 的 `assertOneBotResponseOk` 与上传/回执/健康调用共用。
成功判定 = `status=="ok" && retcode==0`（两桥一致）。

| 形态 | 判读 | 可重试 | 说明 |
|---|---|---|---|
| `status:"ok"` / `retcode:0` | 成功 | — | 附带 `data.message_id`（SnowLuma/NapCat 同形） |
| `retcode:200`（任意 status） | `napcat_rejected` | **否** | NapCat EventChecker 拒发（bot 被禁言/风控，`result:120` 等）。成功形态的拒绝信封，绝不当成功、绝不立即重发 |
| `retcode:100` + wording 含 风控/禁言/封禁/权限/限流/参数 等 | `business` | **否** | SnowLuma `ACTION_FAILED`（QQ/OIDB/桥错误）中的拒发/风控类 |
| `retcode:100` + wording 含 timeout/网络/繁忙/5xx 等 | `business` | 是 | 瞬态业务失败 |
| `retcode:100` 其他 wording | `business` | 是（有界） | 保持历史行为：pRetry 2 次 + outbound 尝试预算（5 次、指数退避），不会风暴 |
| `retcode:1400` | `bad_request` | **否** | SnowLuma 参数/消息段校验失败 —— 修参数，重发无意义 |
| `retcode:1401`（HTTP 401） | `unauthorized` | **否** | token 失效 —— 告警换 token |
| `retcode:1404` | `unknown_action` | **否** | action 不存在（协议不匹配） |
| `retcode:1413`（HTTP 413） | `payload_too_large` | **否** | 请求体过大（内联媒体需降尺寸/改 upload） |
| `retcode:1200`（HTTP 500） | `internal` | 是 | 桥内部错误，可能瞬态 |
| 其他非 0 retcode | `unknown` | 是（有界） | 同 100 其他 wording |
| `status:"failed"` 无 retcode | `unknown` | 是（有界） | 保守沿用旧行为 |
| HTTP 层 408/429/5xx/超时/连接重置 | `transport` | 是 | pRetry 2 次 |
| HTTP 层 其他 4xx | `transport` | **否** | 端点/token/请求错误，重发只会刷日志 |

判读依据（SnowLuma）：`packages/onebot/src/types.ts` RETCODE（0/100/1200/1400/1401/1404/1413）、
`api-handler.ts` "single error seam"（发送失败统一 `status:failed + retcode:100 + wording`）、
element-manifest 明言拒绝 NapCat 那种"成功形态的拒发"信封。
NapCat 侧（现网）：`retcode:200 + EventChecker result:120` 拒发语义 —— SnowLuma **无对应物**
（能力报告 §2.6），故 200 一律按 NapCat 拒发处理，两侧都不会误判。

**重试边界**：`base.ts` `pRetry(retries=2)`（RETRY_LIMIT）只对上表"可重试"错误立即重试；
不可重试错误走 `NonRetryableForwarderSendError` → manager 目标冷却（summary 30min / article 10min）
→ outbound 落 `failed`，由 task-queue claim 的尝试预算（`OUTBOUND_FAILED_RETRY_LIMIT=5`，
指数退避 60s→1h）延迟重投。既无重试风暴，也不永久误丢。

## 2. 媒体来源决策表（`src/services/snowluma-media-source.ts`）

段值由 SnowLuma `pickMediaSource` 解析（`base64://`/`data:` 内联 → `http(s)://` →
`file://`/路径）。`selectQQMediaSource` 为纯函数决策表，`buildQQMediaFileValue` 做 IO
（stat/读文件/分块上传）。**默认 `media_delivery: 'file'`（`file://` 老行为，NapCat 兼容钉）**。

| mode | 决策顺序 | 回退 |
|---|---|---|
| `file`（默认） | `file://<本地路径>` | — |
| `auto` | ① 可拉取 URL（`media_url_base` + 文件名 或媒体自带 url）② `base64://` 内联（≤ `media_inline_max_bytes`，默认 2 MiB）③ `upload_file_stream` 分块上传（≤ `media_upload_max_bytes`，默认 512 MiB） | `file://` |
| `inline` | `base64://` 内联（≤ 上限） | `file://` |
| `upload` | `upload_file_stream` → bot 本地路径 | `file://` |
| `url` | `http(s)://` URL | `file://` |

- 文件不存在/无法 stat：一律 `file://`（保持老行为，由桥报错并带自己的 wording）。
- **大文件红线**：> `media_upload_max_bytes`（默认 512 MiB）绝不走消息体上传，回退 `file://`
  或改走群文件（`upload_group_file`，运维手动）。历史上 3.47 GB 群文件上传超时 900s 的事故
  即由此阈值 + 双超时（单块 `media_upload_timeout_ms=60s`、整体
  `media_upload_deadline_ms=600s`）兜住。
- `upload_file_stream`（NapCat Stream API wire 兼容，两桥通用）：base64 `chunk_data` 分块
  （默认解码 1 MiB/块），末块带 `expected_sha256`，完成返回 `file_path`；
  `file_retention` 默认 1h 后由 bot 自动清理。SnowLuma 侧上限：单块 32 MiB、单流 4 GiB、
  空闲 10 min 收流、HTTP body 64 MiB（413/1413）。
- `url` 模式生命周期：桥可能在发送响应后才拉取，压缩附件副本延迟
  `QQ_MEDIA_URL_CLEANUP_GRACE_MS`（5 min）再清理。
- 静态媒体服务（`media_url_base`）由部署侧提供（如 nginx alias 到
  `CACHE_DIR_ROOT/media`）；未配置时 auto 自动跳过 URL 分支。

## 3. message_id 落库、回执与撤回（P0-3）

- 每次发送的 OneBot `data.message_id` 由 `extractProviderMessageIds`（outbound-message-service）
  归一抽取，写入 **`outbound_messages.message_ids`**（JSON 列，迁移
  `20261014010000_add_outbound_message_ids`）。部分失败（partial）也保留"已可见段"的 id。
  - 迁移/回滚：`ALTER TABLE ... ADD COLUMN`（SQLite，纯增量）。回滚 = 部署旧镜像即可，
    旧代码不读不写该列；无需 down 迁移。
  - 对外 API（`/api/outbound-messages`）经红action 输出 `{redacted_value, id_count}`，不外泄 id。
- 回执核对：`POST /api/actions/qq/receipt-check`（Bearer API secret）
  body `{message_id | message_ids | idempotency_key}` → 逐 id 调 `get_msg`
  （SnowLuma message-store 持久化副本，含 `real_id`）返回 `{ok, messageId, realId, detail}`。
- 撤回：`POST /api/actions/qq/recall`（同 body）→ 逐 id 调 `delete_msg`。
  **默认不自动撤回** —— 发送管线从不调用该能力，仅显式 API/运维调用。
  两接口都接受 `url`/`token` 覆盖（与 `/api/actions/qq/send` 一致），默认取第一个 QQ target。

## 4. 健康探针（P1）

- `GET /api/actions/qq/health?target_id=<id>&url=&token=`：`get_status`（`online` 登录态 +
  `good` 收发链路）+ `get_version_info`（`app_name` 判桥风味：`SnowLuma` / NapCat）。
  给 `target_id` 时写入既有 target-health 表（`last_send_status: 'probe'`），
  `/api/target-health` 可见，不用等一次失败发送才发现桥挂了。
- 判桥风味的纯函数：`detectBridgeFlavor`（onebot-semantics.ts）。

## 5. 切换开关与环境变量

QQ target（`cfg_platform`）：

| 字段 | 默认 | 说明 |
|---|---|---|
| `media_delivery` | `file` | `file \| auto \| inline \| upload \| url` |
| `media_url_base` | 无 | url/auto 的媒体基址 |
| `media_inline_max_bytes` | 2097152 | 内联上限 |
| `media_upload_max_bytes` | 536870912 | 上传上限（> 它回退 file://）|
| `media_upload_chunk_bytes` | 1048576 | 分块大小（解码后）|
| `media_upload_timeout_ms` | 60000 | 单块超时 |
| `media_upload_deadline_ms` | 600000 | 整体上传截止 |
| `media_upload_retention_ms` | 3600000 | bot 侧保留（0=不回收）|

环境变量（覆盖同名 target 配置，便于整机切换、不改 assets/config.yaml）：
`IDOL_BBQ_QQ_MEDIA_DELIVERY`、`IDOL_BBQ_QQ_MEDIA_URL_BASE`、
`IDOL_BBQ_QQ_MEDIA_INLINE_MAX_BYTES`、`IDOL_BBQ_QQ_MEDIA_UPLOAD_MAX_BYTES`、
`IDOL_BBQ_QQ_MEDIA_UPLOAD_CHUNK_BYTES`、`IDOL_BBQ_QQ_MEDIA_UPLOAD_TIMEOUT_MS`、
`IDOL_BBQ_QQ_MEDIA_UPLOAD_DEADLINE_MS`、`IDOL_BBQ_QQ_MEDIA_UPLOAD_RETENTION_MS`。

灰度：`IDOL_BBQ_OUTBOUND_SEND_MODE=capture|blocked` 仍可用于影子对比；换桥回滚 = 改
`url`/`token` 配置 + 重启（端点即配置），媒体回退 = `media_delivery: file`。

## 6. 排障

- WebUI `POST /api/debug/invoke`（SnowLuma 5099，按 UIN 试调任意 action，带文档）：
  用它复现 `send_group_msg`/`get_msg`/`delete_msg`/`upload_file_stream` 的真实返回，
  与本表对照。**write 类 action 只在隔离环境试**。
- 运行时 API：`/api/target-health`（含 probe）、`/api/outbound-messages`（message_id 计数）、
  `tools/forwarder-runtime-api.sh`。
- 雪花标志：`get_version_info.app_name`。非 SnowLuma 时按 NapCat 语义（200/120 分支）走，
  两分支都由判读表覆盖，无需手工开关。

## 7. 有意不做 / 留到下一波

- **不引 `@snowluma/sdk` / `@snowluma/mcp`**：axios + 判读表已覆盖所需语义，引 SDK 会带进
  版本耦合（SnowLuma 文档明言"早期开发、接口可变"）。能力探测已内建（get_version_info）。
  MCP 用于运维查询价值高，但以 `npx @snowluma/mcp` docs 模式侧挂即可，无需入依赖。
- **markdown 段**：当前渲染层产物是图片卡 + 纯文本，无 markdown 输出路径；且 markdown 段的
  QQ 端呈现依赖机器人卡片场景（能力报告标注"推测"）。保持图片卡为默认，待渲染层产出
  markdown 后再接（图片卡路径即回滚路径）。
- **限速**：SnowLuma 发送端限流 [未证实]（能力报告 §2.6），保守维持自建节拍：
  `minInterval` 1s/目标 + pRetry 有界 + 不可重试目标冷却。如需更严可在
  `base.ts` `minInterval` 处加令牌桶（本轮不动）。
- **事件面升级（WS 事件流 / historySync 补缺）**：HTTP 入站事件通道与反应框架已落地
  （见 §8）；WS 事件流与 `historySync` 断线补缺仍未接（能力面未证实事件补发队列，
  历史缺口只能靠 `get_*_msg_history` 事后补），留到下一波。

---

## 8. 入站事件与反应（通用 OneBot v11 事件通道 + 可插拔反应框架）

> 目标口径："高度支持 SnowLuma 特性" = **能反向监控 QQ 群内消息并做出反应**。
> 能力齐备优先，具体业务规则可少。默认**整体关闭**，未配置时现网行为一字不变。

### 8.1 链路图

```
SnowLuma httpClients / NapCat httpPostEndpoints
   │  POST /api/actions/qq/events   （X-Signature: sha1=HMAC-SHA1(secret, rawBody) 或 Bearer）
   ▼
api-manager.handleQqEvents
   ├─ 鉴权（与 x-link 同一 verifier，两桥逐字节同构）
   ├─ normalizeOneBotEvent（services/onebot-event-ingest.ts）
   │     · message: array | CQ-string 双格式（text/@/reply 段、CQ 转义）
   │     · notice/request/meta_event 子类型宽松归一（未知子类型不抛错）
   │     · 输出 {kind, postType, subType, messageType, groupId, userId, sender*,
   │            messageId, text, rawText, atUserIds, atBot, dedupeKey, raw}
   ├─ 幂等去重（EventDedupeCache：TTL + 容量上限；键=message_id/notice/request 唯一键）
   ├─ 自回声丢弃（post_type=message_sent、user_id ∈ bot_user_ids、user_id==self_id）
   ├─ runReactionChain（services/onebot-event-reactions.ts，配置驱动 handler 链）
   │     reply ─────────► send_group_msg / send_private_msg（reply 段=引用回复）
   │     forward/alert ─► 转发到指定群/管理员私聊（可带本地媒体，走 media_delivery 决策表）
   │     command ───────► #status(健康探针) / #ping / #recall <message_id>（管理员白名单）
   │     group_action ──► set_group_ban / set_group_kick / _send_group_notice（默认关闭）
   │     quick_operation► 直接把 {reply, at_sender, delete, ban} 作为 HTTP 响应体交回桥
   └─ 出站调用统一走 callOneBotAction（onebot-receipt-service）
         └── 失败判读 = §1 判读表（onebot-semantics），单发不重试
```

**与 x-link 的关系**：`POST /api/actions/qq/x-link`（贴 X 链接→立即转帖）是**独立入口，零改动**。
SnowLuma 侧保留原有的 `idol-bbq-x-link` httpClients 推送不变，另加一条 `idol-bbq-events`
指向 `/api/actions/qq/events`。理由：x-link 对外行为一字不变（回归测试钉住），推送分离
互不干扰、回滚只需摘一条 httpClients；事件通道内**不**复刻 X 链接转帖（避免双触发）。

### 8.2 事件清单（两方言同一归一出口）

| post_type | 覆盖 | 备注 |
|---|---|---|
| `message` | ✅ | `message_type: group/private`（群临时会话归 private + group_id）；`message` 为 array 或 CQ 字符串均可 |
| `message_sent` | ✅（丢弃） | SnowLuma `reportSelfMessage` 出站回声，永远不触发反应 |
| `notice` | ✅ | 兼容 NapCat 常用子类型 + SnowLuma 新增（`group_msg_emoji_like`、`essence`、`notify/input_status`、`bot_offline`、`group_card`…）；未知子类型归一为 `notice` + 保留 raw |
| `request` | ✅ | `friend` / `group`（含 `flag`、`comment`），可配规则转发给管理员 |
| `meta_event` | ✅（不触发） | heartbeat/lifecycle 归一但默认无 handler 匹配（post_types 默认 `['message']`） |
| 未知 post_type | ✅（不触发） | 归一为 `kind: 'unknown'`，记录后跳过，**不崩** |

### 8.3 规则 schema（`qq_inbound`，顶层配置键）

见 `assets/tweet-forwarder/config.example.yaml` 末尾完整注释示例。要点：

- `enabled`：主开关，缺省/`false` = 端点只回 `ignored`，零副作用。
- `bot_user_ids` / `admins`：自回声白名单 / 管理员白名单（命令与 group_action 鉴权）。
- `allow_group_actions`：群管理反应的**第二道**开关（第三道是 handler 的 group 允许列表）。
- `dedupe_ttl_seconds`（默认 3600）/ `dedupe_max_entries`（默认 4096）：幂等窗口。
- `default_cooldown_seconds` / `default_max_per_hour`：每 handler × 每群/私聊 的冷却与小时配额，
  handler 可用 `cooldown_seconds` / `max_per_hour` 覆盖。
- `handlers[]`：`id`、`type`（`reply | forward | command | group_action | quick_operation`）、
  `enabled`、`match`（`post_types`、`message_types`、`group_ids`、`user_ids`、`text_regex`、
  `raw_regex`、`at_bot`、`require_admin`）、各 type 的专属配置块、可选 `url`/`token` 覆盖出站端点。
- 模板占位符：`{text}` `{raw_text}` `{user_id}` `{group_id}` `{sender_name}` `{message_id}` `{handler_id}`。

### 8.4 鉴权

| 方式 | 头 | 说明 |
|---|---|---|
| 桥签名（推荐给 webhook） | `X-Signature: sha1=<hex>` | `sha1=HMAC-SHA1(api.secret, rawBody)`，与 x-link 同一 verifier（timing-safe）；SnowLuma `httpClients.accessToken` = `api.secret` 即可 |
| Bearer | `Authorization: Bearer <api.secret>` | 运维手工 POST / 测试用 |

两桥（SnowLuma / NapCat）签名逐字节同构，无需分支。鉴权失败一律 **401**（不泄露原因）。

### 8.5 幂等（webhook 重发不重复反应）

- 键：`message` 事件 = `msg:<self_id>:<message_id>`；`notice`/`request` 用
  `message_id/flag + group/user/operator/target` 组合键；无 id 时回退 raw 哈希。
- 存储：进程内 TTL + 容量上限缓存（`onebot-event-dedupe.ts`）。
  **有意不做 DB 表**：默认关闭的功能不值得一条迁移；代价是重启后最多重放一个 TTL 窗口
  （按桥的重发窗口选 1h 足够）。
- 响应语义：重复投递回 `{"status":"duplicate"}`，不进 handler 链。

### 8.6 防呆

| 风险 | 对策 |
|---|---|
| 自触发死循环（bot 回复又触发自己） | `post_type=message_sent`、`user_id ∈ bot_user_ids`、`user_id == self_id` 一律丢弃；上游 `reportSelfMessage:false` 也关掉回声 |
| 反应刷屏 | 每 handler × 每群/私聊：`cooldown_seconds` 冷却 + `max_per_hour` 配额（超限记 `limited` 并跳过） |
| 重试风暴 | 每个反应**单发一次**，不自动重试；失败按 §1 判读表分类记录（`failed` + wording），可重试语义留给出站侧既有预算 |
| 坏事件崩服务 | 归一化失败 → `ignored`；handler 异常 → try/catch 隔离，链上后续 handler 照常执行 |
| webhook 重发 | §8.5 幂等 |
| 群管理误伤 | `group_action` 三重开关（`allow_group_actions` + handler 允许列表 + 管理员触发者），默认全关 |
| 非管理员滥用命令 | `command` 默认 `admin_only: true`；`#recall` **永远**管理员白名单 |

### 8.7 quick operation（SnowLuma 快速操作）

SnowLuma 支持在 HTTP 上报的**响应体**里直接回快速操作（`network/quick-operation.ts`，
能力报告 §3.2 [确认]；支持 `reply`（群聊自动 @发送者）、`delete`、`ban` 等）。
`quick_operation` handler 命中后，端点把 `{reply, at_sender, delete, ban}` **作为响应体**
原样返回（不再是 `{success,...}` 信封），桥侧直接执行，省一次显式发送调用。
注意：真机效果以登录后验收为准（本次未登录，属"文档/源码级确认"）。

### 8.8 如何加新 handler

1. `src/services/onebot-event-reactions.ts`：在 `QqInboundHandlerType` 加类型名，
   在 `runReactionChain` 的 switch 里加分支（保持单发、不抛错、返回 `ReactionActionResult`）。
2. 配置块加到 `QqInboundHandlerConfig`（含 JSDoc）与 `config.example.yaml` 示例。
3. 出站一律复用 `callOneBotAction` / `recallOneBotMessage` / `probeQqBridgeHealth`
   （失败判读自动走 §1 判读表）；需要媒体时复用 `buildQQMediaFileValue`（§2 决策表）。
4. 测试：`src/services/onebot-event-reactions.test.ts` 加命中/不命中/失败隔离/限速各一钉。

### 8.9 验证与回滚

- 单元/集成测试：`onebot-event-ingest.test.ts`（两方言解析）、`onebot-event-dedupe.test.ts`
  （幂等+限速）、`onebot-event-reactions.test.ts`（规则/防呆/隔离）、
  `api-manager.test.ts`（鉴权双兼容、幂等、quick op、坏事件、自回声、x-link 不受影响）。
- 上机验证（3020e）：SnowLuma 容器内 `node -e "fetch('http://host.docker.internal:3000/api/actions/qq/events',{...X-Signature...})"`
  → 200 才算通；`/home/sumie/snowluma/verify-e2e.sh` 含入站冒烟步骤。
- 回滚三步：① `qq_inbound.enabled` 不配/置 false（立即静默）② `git revert` 该 commit
  ③ 摘掉 SnowLuma `onebot.json` 的 `idol-bbq-events` httpClients 条目 + 回滚镜像锚点。
