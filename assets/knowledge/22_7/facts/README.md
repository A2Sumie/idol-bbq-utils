# 22/7 Facts Knowledge Assets

These files store reusable 22/7 factual context for translation and schedule
understanding prompts. They are source-backed facts, not model activation prose.

## Files

- `members.json`: current official member profiles from the 22/7 official mobile
  profile pages, including birthdays, member colors, origin, height, SNS links,
  character links when assigned, and source metadata.
- `characters.json`: official character profiles linked from current members,
  including birthdays, penlight colors, CV/member mapping, and source metadata.
- `discography.json`: Sony Music official discography data, including edition
  details, grouped releases, and a unique track-title index with first release
  metadata.
- `performances.json`: official 22/7 performance/date facts, with a curated
  confirmed date-event timeline plus crawled official live-report references.
- `prompt-facts-brief.txt`: compact member, character, single/album release,
  and performance-date facts for prompt injection.
- `song-title-index.txt`: prompt-friendly official track title index.
- `release-calendar.txt`: prompt-friendly release-date calendar.
- `performance-calendar.txt`: prompt-friendly performance/date calendar. It
  intentionally omits detailed open/start/end times.
- `source-index.json`: source policy and discovery notes.

## Source Policy

Stored factual assertions are sourced from official pages only:

- 22/7 official mobile member/character pages:
  `https://nanabunnonijyuuni-mobile.com/s/n110/artist/a22?ima=4750`
- Sony Music official discography page and JSONP API:
  `https://www.sonymusic.co.jp/artist/nanabunnonijyuuni/discography/`
- 22/7 official news/schedule/live-report pages for live and performance dates.

Search results are used to discover official URLs and identify possible gaps.
Secondary sources such as fan wikis or profile aggregators are not imported as
authoritative facts unless they are explicitly promoted and annotated later.

## Refresh

Run from the repository root:

```sh
node tools/refresh-22-7-knowledge.mjs
```

After refresh, validate with:

```sh
jq empty assets/knowledge/22_7/facts/*.json
jq '.count' assets/knowledge/22_7/facts/members.json
jq '.counts' assets/knowledge/22_7/facts/discography.json
jq '.counts' assets/knowledge/22_7/facts/performances.json
```

## Prompt Use

For translation prompts, prefer injecting `prompt-facts-brief.txt` and only the
needed slice of `song-title-index.txt`. Use `release-calendar.txt` and
`performance-calendar.txt` when a task needs fuller date coverage. Use JSON files
for programmatic lookup or when a prompt needs exact source metadata.
