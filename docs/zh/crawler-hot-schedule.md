# Crawler Hot Schedule

本项目的 crawler 排程不再依赖为每个 crawler 启一个 CronJob。运行时使用非 Cron 的 slot/window 计划：

- `cfg_crawler.schedule.slots`: 固定日内时间点，例如 `["18:05", "18:20"]`。
- `cfg_crawler.schedule.windows`: 重复窗口，例如 `{ start: "18:05", end: "22:45", every_minutes: 15 }`。
- `cfg_crawler.schedule.min_gap_seconds`: 同一 crawler 两次触发的最小间隔。
- `cfg_crawler.schedule.jitter_seconds`: 稳定抖动，按 crawler/time 派生，不是随机漂移。
- 旧 `cfg_crawler.cron` 只作为兼容输入，会被展开成日内 slot；生产配置应逐步迁移到 `schedule`。

临时插点走 `task_queue` 的 `scheduled_crawler_run` 类型，使用 `execute_at` 和幂等键持久化。SpiderTaskScheduler 会 claim 到期任务，然后复用现有 `spider:task:dispatch` lifecycle，并把 `task_queue_id` 带进任务 meta。

## API

```http
GET /api/schedules/crawlers
GET /api/schedules/crawlers/recommendations?days=120
POST /api/schedules/crawlers/upsert
POST /api/schedules/crawlers/insert
```

`upsert` 示例：

```json
{
  "crawler": "YouTube抓取 - 22点补扫",
  "schedule": {
    "timezone": "Asia/Tokyo",
    "windows": [
      { "start": "22:13", "end": "22:43", "every_minutes": 30 }
    ],
    "min_gap_seconds": 300
  }
}
```

`insert` 示例：

```json
{
  "crawler": "22/7官网FC抓取 - 日间轮询",
  "delay_seconds": 90,
  "reason": "operator spot check after official site update"
}
```

## MCP Tools

- `idol_bbq_crawler_schedule_status`
- `idol_bbq_crawler_schedule_recommendations`
- `idol_bbq_crawler_schedule_insert`
- `idol_bbq_crawler_schedule_upsert`

## Current Dense Stable Windows

Based on the 3020e production DB, 120-day JST article timestamp distribution:

- X: keep all-day 4-minute scans per list with the two list crawlers offset by 2 minutes. Dense activity is strongest at 18:00-00:59 and 12:00; production lag showed the old 8-minute per-list cadence commonly planned sends around 9 minutes after source time.
- Instagram: keep 15:00-23:59 every 5 minutes and 00:00-14:59 every 10 minutes. The densest buckets are 19:00-23:59, especially 21:00-23:45; production lag showed the old 15/30-minute cadence commonly planned sends 30+ minutes after source time.
- TikTok: keep all-day 8-minute scan. Observed density is concentrated at 18:00-23:59, but cross-platform duplicate/video handling benefits from prompt capture.
- YouTube: keep all-day 10-minute scan; add/keep offset scans for 18:00-22:59. Production data shows 22:00-22:45 as a real update band.
- Website/FC: keep official blog 10:00-23:59 every 10 minutes and FC broad scan 10:00-23:59 every 30 minutes; add/keep offset scans for 18:00-22:59. Old `00:00` website buckets are date-only artifacts, not true publish times.
