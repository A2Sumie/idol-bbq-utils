# IG Cookie 轮换守则

依据：Instagram Android 443.0.0.48.82 / iOS 442.0.0 双端静态逆向（2026-08 收官，
`scratch/instagram-re/spider-intel.md` §1.1）。本文是操作守则，不涉及代码改动——
现有单 profile 单账号模式天然满足这些约束。

## 承重 cookie 与设备级语义

| Cookie | 语义 | 轮换守则 |
|---|---|---|
| `sessionid` / `ds_user_id` | 账号身份主凭据 | 账号级：换账号时随之更换；死亡不可静默恢复（app 无静默重登类，检测到死就早换） |
| `csrftoken` | POST 防 CSRF | 每会话轮换；我们代码发起的 `web_profile_info` 是 GET，无需 `X-CSRFToken`，不动 |
| `mid` / `ig_did` | **设备指纹** | **设备级**：Android 登出时设备 ID 不清除（实测），换账号 cookie 时**保留原值** |
| `datr` | 浏览器/设备标识（FB 系） | **设备级**（iOS keychain `com.facebook.datr` 实测）：同上保留 |
| `rur` / `shbid` / `shbts` | 路由/区域提示 | 服务端经响应头轮换，无需手动维护 |

## 换账号操作步骤

在同一浏览器 profile 里更换账号 cookie 时：

1. **保留** `mid`、`ig_did`、`datr`（它们属于"这台设备"，不属于这个账号）；
2. **替换** `sessionid`、`ds_user_id`、`csrftoken`；
3. 不要清整个 cookie jar——全量清空等于每次换"一台新设备"，比保留旧 mid 更可疑。

## 环境一致性（比换 cookie 更重要）

一套 cookie 绑定固定 **profile + UA + IP**。出现 `delta_login_review` challenge
（新环境审查）说明环境指纹变了而非 cookie 过期——**换回原环境比换 cookie 有效**
（intel §1.3）。表现为响应体 `login_required`/`checkpoint_required` 且 challenge
上下文含 `delta_login_review`；spider 侧会以
`instagram_session_dead ... hint=environment-changed` 记入日志。

## 相关实现

- 会话死亡三谓词检测：`core/spider/src/spiders/instagram.ts`（graphql gate 响应体检测）
- `X-IG-WWW-Claim` 回放（会话自愈）：同上（仅对代码自身发起的 `api/v1` 请求）
- 会话死亡后的 cooldown：`app/tweet-forwarder/src/managers/spider-manager.ts`（classify → auth）
