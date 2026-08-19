# Instagram persisted docId table (offline fallback, do not import)

`ig4a-instagram-schema_client_persist_ids.json` — 明文 op→docId 全量表，**拷贝自**
Instagram Android 443.0.0.48.82 (APKM) 的 APK assets（2026-08 逆向收官时留档）。
sha256 `5df589e20ff1b54b81021485b1256b4362e214a3b134882e1a80da5cab073a58`。

- 结构：`{operation_name: {operation_name_hash, operation_text_hash, client_doc_id, schema}}`
- 规模：1683 条 operation
- 例：`IGLoginGenerateCodeMutation → 82937153511246572586758342999`

## 用途（备胎，未接线）

**本文件当前没有任何代码引用**（刻意为之：不进 bundle，不参与构建）。它是 API 面的
退路留档：若哪天现行 web 调用形状（POST `/graphql/query` 表单
`fb_api_req_friendly_name=<OpName>`）被服务端收紧，可改从页面上下文带 cookie 直发：

```
POST https://www.instagram.com/api/v1/wwwgraphql/ig/query/
Content-Type: application/x-www-form-urlencoded

doc_id=<client_doc_id>&variables=<json>
```

docId 是客户端预编译的稳定键，不随请求变化（intel §2.2）。

## 注意

- 只有 Android assets 有明文 docId 表；iOS Hermes 内无 docId 常量（native 解析）。
- web 直播页的 friendly-name 不在本表内（web JS 服务端下发）。
- 需要 `doc_id` 调用形状时再实现调用路径；不要预防性地接入。
