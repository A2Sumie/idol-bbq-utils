# B 站投稿标题策略（biliup 标题生成 / 译题）

适用范围：`app/tweet-forwarder/src/middleware/forwarder/biliup.ts` 的投稿标题、简介与标签元数据。
核心口径：**B 站投稿的主标题 = 原标题的翻译**。有原标题的来源（YouTube 视频/Shorts）一律“翻译优先于生成”，
不允许模型另起炉灶写一个新标题；没有原标题的来源（X / Instagram / TikTok 文案）才走“概括”路径。

## 标题格式模板

统一格式（所有来源，代码默认值与 `assets/config.yaml` 的 `metadata_templates.title` 一致）：

```
【{{account_title}}】[{{source_tag}}] {{upload_summary}}   →   【22/7 北原実咲】[X] 06.17_26 <标题正文>
```

- `【22/7 …】` 品牌前缀：`22/7` + 成员/账号显示名（命中 `assets/knowledge/22_7/facts/members.json` 的成员名）。
  账号无法识别时退化为 `【22/7】`。它是 B 站搜索锚点，保留。
- `[{{source_tag}}]` **条件化来源标记**（见下节）：渲染为 `[YT]`/`[X]`/`[ins]`/`[TT]`/`[blog]` 或**空**；
  为空时 `[ ]` 槽位连同后随空格一起消失，标题与不带槽位的模板逐字一致。
- `MM.DD_YY` 发布日期码（如 `06.17_26`）：按时间浏览、同题去重用，保留。
- 标题正文：YouTube = **原标题的中文翻译**；其它来源 = 正文开头事实的中文翻译/浓缩。

### 来源标记条件化（何时带 `[来源]`、何时不带）

口径：**标题能自证出处就不带来源标记，标题是概括/兜底出来的才带**。

| 来源 / 标题形态 | 来源标记 | 例 |
|---|---|---|
| X / Instagram / TikTok（标题=正文概括，无出处辨识度） | **保留** `[X]`/`[ins]`/`[TT]` | `【22/7 河瀬詩】[X] 06.17_26 今天有演出` |
| YouTube Shorts | **保留** `[YT]` | `【22/7】[YT] 06.17_26 短视频一` |
| YouTube 无原标题（content 首行不是创作者标题） | **保留** `[YT]` | `【22/7】[YT] 06.17_26 直播后的感谢` |
| 兜底日期壳标题（正文清洗后为空，`upload_summary` 只剩 `MM.DD_YY`，如 `【22/7】03.20_24`） | **保留**（含 YouTube 长视频退化到日期壳时） | `【22/7】[YT] 06.17_26` |
| **YouTube 长视频**（type ≠ `shorts`）且 content 首行有原标题，标题=原标题译文 | **省略** | `【22/7】06.17_26 新曲《二つの道》音乐MV` |
| website / blog 及其它（不在省略清单） | **保留** `[blog]`/`[社媒]` | — |

实现：`resolveTitleSourceTag(article, hasTitlePayload)`（`biliup.ts`）决定 `{{source_tag}}` 渲染成裸标签或空串；
空槽清理见 `stripEmptyRenderedBrackets`（空 `[…]` 连同一个后随半角空格一起删）。
`source_context.source_tag`（LLM 元数据输入）始终是**真实来源标签**，不受此条件影响。
确定性兜底标题（`buildTitleFallback`）走同一条件，与模板渲染一致。

### 旧模板 → 新模板

| | 旧 | 新 | 理由 |
|---|---|---|---|
| YouTube 长视频（原标题译文） | `【22/7 …】MM.DD_YY <生成标题>` | `【22/7 …】MM.DD_YY <原标题译文>` | 用户要求主标题=原标题翻译；生成标题质量差 |
| Shorts / 无原标题 YT / X / IG / TT / 日期壳 | `【22/7 …】[YT]/[X]/[ins]/[TT] MM.DD_YY <正文>` | 同旧（**保留来源标记**） | 标题是概括/兜底出来的，无出处辨识度，来源标记留辨识度 |
| YouTube 长视频（原标题译文）来源标记 | `…[YT] MM.DD_YY <译文>` | `…MM.DD_YY <译文>`（**省略**） | 标题本身自证出处；来源在简介 `来源平台:` 行完整署名 |

## 译题（translate_original_title）规则

- `original_title`（content 首行）是**唯一翻译素材**：忠实翻译，不改写、不扩写、不概括、不补写原文没有的信息。
- 保留成员名、角色名、作品名、曲名、节目名、数字、日期、期数、序号与原文结构；不截断、不漏译关键名词。
- 专有名词按译名表（`assets/knowledge/22_7/translation/translation_terms_22_7_ja_zh.json`）的 `target` 写法，
  随 prompt 输入（`glossary` 字段）下发并全篇一致；译名表未收录的专有名词保守处理（可保留原文）。
  - 例：天城サリー → 天城莎莉、北原実咲 → 北原实咲、計算外 → 计算外、ナナニジハウス → 七虹家。
  - 歌名/节目名按表内 note 保留日文（如「命の続き」「蝉は夏を知らない」），中文标题里出现这些日文专名是合法的。
- 自然简体中文，不逐词硬译、不带翻译腔、不标题党（禁“震惊/必看/神回”类词）、不加译注。
- 原文原标题照旧放简介首行：`原标题: <原文>`。

## 概括（summarize_post_title）规则

- 来源无独立标题：把正文开头主要信息忠实翻译并浓缩为中文标题；不得脑补。
- 不得用“成员名+日期”空壳当标题主体；标题主体必须反映内容本身。
- 模型若把账号名/日期回显进标题，确定性后处理会剥掉开头上下文（translate 模式不剥，译文保持忠实）。

## 确定性后处理（与模型无关，始终生效）

1. **空白/括号规范化**：去控制符、合并空白、剥无配对书名号/引号。
2. **平台词/搬运词清洗**：标题正文里删除 `YouTube/Twitter/TikTok/Instagram/推特/搬运/转载/…` 等平台与搬运属性词
   （缩写 `yt/tt/x/…` 仅当独立成词时删除），B 站标题不带平台/搬运标记。
3. **emoji 收敛**：连续 3 个以上 emoji 折叠为 2 个；正常符号（♪、～、【】、#tag# 等）保留。
4. **安全截断**：超过 B 站 80 字上限（码点计）时按语义边界（空格/分隔符/收尾括号）截断并加单个 `…`，
   绝不切半个词/人名；无边界时在预算内硬截。
5. **兜底链路**（LLM 不可用/译文不合格时）：
   `LLM 译题 → article.translation 首行（content 整体译文的首行=标题译文）→ 原标题原文 → 【22/7】MM.DD_YY 日期壳`。
   空标题、纯日期、纯标点一律走壳/硬兜底（`平台标签 + datetime + article_id`），不产出空壳正文；
   **日期壳/硬兜底标题一律保留来源标记**（无任何内容辨识度）。
6. **译文合格性校验**：无汉字或日文假名占绝对多数（模型回显原文未译）→ 拒收走兜底；标题党禁词 → 拒收。

## 标签（tags）

- 固定标签保留；优先 成员名/作品名/节目名/活动主题/声优偶像/日系偶像；每标签 ≤20 字，最多 10 个。
- 平台/搬运属性词禁入标签（与标题清洗同表思路）。
- 标签与标题可共用一次 LLM 调用（同 provider/key 时合并），输入 JSON 结构：

```json
{
  "task": "translate_original_title | summarize_post_title | generate_tags_only",
  "original_title": "仅 translate 模式：原标题（唯一翻译素材）",
  "text": "正文文本块",
  "glossary": [{ "source": "天城サリー", "target": "天城莎莉", "note": "..." }],
  "current_tags": ["..."],
  "target_count": 10,
  "title_min_chars": 8,
  "title_max_chars": 32,
  "source_context": { "platform": "...", "source_tag": "...", "date": "...", "...": "..." }
}
```

`title_min_chars` / `title_max_chars` 是给模型的**目标长度参考**（translate 模式允许更短的忠实译文），
硬约束只认 B 站 80 字上限与后处理的安全截断。

## 配置

`assets/config.yaml` → `forwarders.biliup.video_upload`：

- `metadata_templates.title`：默认 `【{{account_title}}】[{{source_tag}}] {{upload_summary}}`（自定义模板仍受支持；
  `{{source_tag}}` 按“来源标记条件化”表渲染为裸标签或空串，空槽连同后随空格一起消失）。
- `title_generation`：`enabled` / 独立 `provider`+`api_key`（缺省复用 `tag_generation` 凭据）/
  `target_min_chars` / `target_max_chars` / `cfg_processor`。`title_generation: false` 可整体关闭译题，
  标题回落到确定性模板路径。
- `cfg_processor.prompt` 若配置则整体覆盖默认 prompt（译题/概括规则随覆盖失效）。
