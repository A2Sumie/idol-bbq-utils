#!/usr/bin/env node

import { mkdir, writeFile } from "node:fs/promises";

const OFFICIAL_MEMBER_SEED =
  "https://nanabunnonijyuuni-mobile.com/s/n110/artist/a22?ima=4750";
const OFFICIAL_MEMBER_BASE = "https://nanabunnonijyuuni-mobile.com";
const SONY_DISCO_LIST =
  "https://www.sonymusic.co.jp/json/v2/artist/nanabunnonijyuuni/discography/start/{start}/count/{count}/callback/callback";
const SONY_DISCO_DETAIL =
  "https://www.sonymusic.co.jp/json/v2/artist/nanabunnonijyuuni/discography/{code}/callback/{callback}";
const OUTPUT_DIR = new URL("../assets/knowledge/22_7/facts/", import.meta.url);
const OFFICIAL_LIVE_REPORT_LIST =
  "https://nanabunnonijyuuni-mobile.com/s/n110/diary/special/list?cd=special";
const OFFICIAL_PERFORMANCE_EVENTS = [
  {
    id: "2026-01-29-nananiji-live-2026-1",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」#1",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-01-29",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: ["official schedule item; live report title confirms #1 and 1月29日公演"],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_1",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9193?dy=20260129",
      },
      {
        id: "22_7_official_live_report_nananiji_live_2026_1",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/446422?cd=special",
      },
    ],
  },
  {
    id: "2026-01-30-nananiji-live-2026-2",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」#2",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-01-30",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: ["official schedule item; live report title confirms #2 and 1月30日公演"],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_2",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9194?dy=20260130",
      },
      {
        id: "22_7_official_live_report_nananiji_live_2026_2",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/446543?cd=special",
      },
    ],
  },
  {
    id: "2026-02-23-nananiji-live-japan-festa",
    title: "22/7リアル公演「ナナニジライブ inジャパンフェスタ」",
    kind: "live",
    period: "nananiji_live_2026",
    date: "2026-02-23",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "西新井文化ホール",
    lineup_note: "リアル公演",
    notes: ["official schedule item; official live report published 2026-02-27"],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_japan_festa",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9191?dy=20260223",
      },
      {
        id: "22_7_official_live_report_nananiji_live_japan_festa",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447065?cd=special",
      },
    ],
  },
  {
    id: "2026-02-25-nananiji-live-2026-3",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」#3",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-02-25",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: ["official schedule item; live report title confirms #3 and 2月25日公演"],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_3",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9195?dy=20260225",
      },
      {
        id: "22_7_official_live_report_nananiji_live_2026_3",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447239?cd=special",
      },
    ],
  },
  {
    id: "2026-02-26-nananiji-live-2026-4",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」#4",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-02-26",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: ["official schedule item; live report title confirms #4 and 2月26日公演"],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_4",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9196?dy=20260226",
      },
      {
        id: "22_7_official_live_report_nananiji_live_2026_4",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/447855?cd=special",
      },
    ],
  },
  {
    id: "2026-04-04-live-15",
    title: "22/7 LIVE「15」",
    kind: "live",
    period: "fifteen_member_live",
    date: "2026-04-04",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "東京国際フォーラム ホールC",
    lineup_note: "15人体制ライブ",
    notes: ["追加公演"],
    sources: [
      {
        id: "22_7_official_schedule_live_15",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9279?dy=20260404&ima=0000&link=ROBO004",
      },
      {
        id: "22_7_official_live_report_live_15",
        type: "official_live_report",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/diary/detail/448820?cd=special",
      },
    ],
  },
  {
    id: "2026-05-11-nananiji-live-2026-5",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-05-11",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: [],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_20260511",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9369?dy=20260511",
      },
    ],
  },
  {
    id: "2026-05-12-nananiji-live-2026-6",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」",
    kind: "regular_live",
    period: "nananiji_live_2026",
    date: "2026-05-12",
    date_kind: "performance_date",
    date_precision: "day",
    venue: null,
    lineup_note: "3期生定期公演",
    notes: [],
    sources: [
      {
        id: "22_7_official_schedule_nananiji_live_2026_20260512",
        type: "official_schedule",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/media/detail/9370?dy=20260512",
      },
    ],
  },
  {
    id: "2026-05-24-fivesta",
    title: "22/7 LIVE「Fivesta」",
    kind: "live",
    period: "second_generation_five_member_stage",
    date: "2026-05-24",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "西新井文化ホール",
    lineup_note: "2期生5人: 相川奈央・麻丘真央・椎名桜月・月城咲舞・望月りの",
    notes: ["22/7初のカメコ席販売あり"],
    sources: [
      {
        id: "22_7_official_news_fivesta",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11193?ima=1105",
      },
      {
        id: "galaxcity_official_event_fivesta",
        type: "official_venue_page",
        url: "https://www.galaxcity.jp/cultural-hall/event/20260524-008515.html",
      },
    ],
  },
  {
    id: "2026-06-08-fivesta-reflection-stream",
    title: "ナナニジハウス限定 22/7 LIVE「Fivesta」振り返り生配信",
    kind: "fc_stream",
    period: "second_generation_five_member_stage",
    date: "2026-06-08",
    date_kind: "stream_date",
    date_precision: "day",
    venue: "ナナニジハウス",
    lineup_note: "FC限定番組",
    notes: ["Fivesta振り返り生配信"],
    sources: [
      {
        id: "22_7_official_news_fivesta_reflection_stream",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11328?ima=0000&link=ROBO004",
      },
    ],
  },
  {
    id: "2026-06-25-nananiji-live-2026-final",
    title: "22/7 3期生 定期公演「ナナニジライブ2026」Final",
    kind: "regular_live_final",
    period: "nananiji_live_2026",
    date: "2026-06-25",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "ヒューリックホール東京",
    lineup_note: "3期生定期公演 Final",
    notes: ["3期生にとって初のヒューリックホール東京公演"],
    sources: [
      {
        id: "22_7_official_news_nananiji_live_2026_final",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11253?ima=0000&link=ROBO004",
      },
      {
        id: "22_7_official_news_nananiji_live_2026_final_general_sale",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11319?ima=0000&link=ROBO004",
      },
    ],
  },
  {
    id: "2026-07-24-atc-special-stage-taipei-fan-meeting",
    title: "ATC Special Stage 22/7 ファンミーティング",
    kind: "fan_meeting",
    period: "live_in_taipei_2026",
    date: "2026-07-24",
    date_kind: "event_date",
    date_precision: "day",
    venue: "2026 第25回漫画博覧会 メインBステージ / 台北世界貿易センター第1展示ホール",
    lineup_note: "麻丘真央および3期生の出演なし",
    notes: ["22/7 Live in Taipei 2026前夜祭的企画"],
    sources: [
      {
        id: "22_7_official_news_taipei",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11315?ima=0037",
      },
    ],
  },
  {
    id: "2026-07-25-live-in-taipei",
    title: "22/7 Live in Taipei 2026",
    kind: "live",
    period: "live_in_taipei_2026",
    date: "2026-07-25",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "Clapper Studio",
    lineup_note: "3期生の出演なし",
    notes: ["22/7初の海外単独公演"],
    sources: [
      {
        id: "22_7_official_news_taipei",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11315?ima=0037",
      },
    ],
  },
  {
    id: "2026-11-07-anniversary-live-2026",
    title: "22/7 ANNIVERSARY LIVE 2026",
    kind: "anniversary_live",
    period: "anniversary_live_2026",
    date: "2026-11-07",
    date_kind: "performance_date",
    date_precision: "day",
    venue: "東京国際フォーラム ホールA",
    lineup_note: "約4年ぶりの東京国際フォーラム ホールA公演",
    notes: [],
    sources: [
      {
        id: "22_7_official_news_anniversary_live_2026",
        type: "official_page",
        url: "https://nanabunnonijyuuni-mobile.com/s/n110/news/detail/11252",
      },
    ],
  },
];

const userAgent =
  "Mozilla/5.0 (compatible; idol-bbq-utils knowledge refresh; +https://github.com/idol-bbq-utils/idol-bbq-utils)";

async function fetchText(url) {
  const response = await fetch(url, {
    headers: {
      "user-agent": userAgent,
      accept: "text/html,application/json;q=0.9,*/*;q=0.8",
    },
  });
  if (!response.ok) {
    throw new Error(`Fetch failed: ${response.status} ${response.statusText} ${url}`);
  }
  return response.text();
}

function decodeHtml(value = "") {
  return value
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">")
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'")
    .replace(/&#x([0-9a-fA-F]+);/g, (_, hex) =>
      String.fromCodePoint(Number.parseInt(hex, 16)),
    )
    .replace(/&#(\d+);/g, (_, dec) => String.fromCodePoint(Number.parseInt(dec, 10)));
}

function stripTags(value = "") {
  return decodeHtml(value)
    .replace(/<br\s*\/?>/gi, "\n")
    .replace(/<\/p>\s*<p[^>]*>/gi, "\n")
    .replace(/<[^>]+>/g, "")
    .replace(/\r/g, "")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n[ \t]+/g, "\n")
    .replace(/[ \t]{2,}/g, " ")
    .trim();
}

function firstMatch(html, pattern) {
  const match = html.match(pattern);
  return match ? stripTags(match[1]) : "";
}

function attrFirst(html, pattern) {
  const match = html.match(pattern);
  return match ? decodeHtml(match[1]) : "";
}

function absoluteUrl(url) {
  if (!url) return "";
  return new URL(decodeHtml(url), OFFICIAL_MEMBER_BASE).href;
}

function canonicalMemberUrl(url) {
  const full = absoluteUrl(url);
  const parsed = new URL(full);
  const code = parsed.pathname.match(/\/artist\/(a\d+)/)?.[1];
  return code ? `${OFFICIAL_MEMBER_BASE}/s/n110/artist/${code}` : full;
}

function canonicalCharacterUrl(url) {
  const full = absoluteUrl(url);
  const parsed = new URL(full);
  const code = parsed.pathname.match(/\/artist\/(c\d+)/)?.[1];
  return code ? `${OFFICIAL_MEMBER_BASE}/s/n110/artist/${code}` : full;
}

function parseDl(html) {
  const entries = {};
  const pattern = /<dt[^>]*>([\s\S]*?)<\/dt>\s*<dd[^>]*>([\s\S]*?)<\/dd>/gi;
  for (const match of html.matchAll(pattern)) {
    entries[stripTags(match[1])] = stripTags(match[2]);
  }
  return entries;
}

function parseMonthDay(value) {
  const match = value.match(/(\d{1,2})月(\d{1,2})日/);
  if (!match) return null;
  return `${match[1].padStart(2, "0")}-${match[2].padStart(2, "0")}`;
}

function parseHeightCm(value) {
  const match = value.match(/(\d+(?:\.\d+)?)\s*cm/i);
  return match ? Number(match[1]) : null;
}

function parseSns(html) {
  const snsBlock = html.match(/<ul class="artist_detail_sns[\s\S]*?<\/ul>/i)?.[0] ?? "";
  const links = [];
  for (const match of snsBlock.matchAll(/<a\s+href="([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi)) {
    const icon = attrFirst(match[2], /alt="([^"]*)"/i);
    links.push({
      platform: icon || inferPlatform(match[1]),
      url: decodeHtml(match[1]),
    });
  }
  return links;
}

function inferPlatform(url) {
  if (/x\.com|twitter\.com/i.test(url)) return "x";
  if (/instagram\.com/i.test(url)) return "instagram";
  if (/youtube\.com|youtu\.be/i.test(url)) return "youtube";
  if (/tiktok\.com/i.test(url)) return "tiktok";
  return "web";
}

function parseMemberIndex(seedHtml) {
  const thirdMarker = seedHtml.indexOf("22/7_the 3rd");
  const memberLinks = [];
  const seen = new Set();
  for (const match of seedHtml.matchAll(/href="([^"]*\/s\/n110\/artist\/a\d+[^"]*)"/g)) {
    const href = decodeHtml(match[1]);
    const code = href.match(/\/artist\/(a\d+)/)?.[1];
    if (!code || seen.has(code)) continue;
    seen.add(code);
    const index = match.index ?? 0;
    memberLinks.push({
      code,
      url: canonicalMemberUrl(href),
      official_section:
        thirdMarker >= 0 && index > thirdMarker ? "22/7_the_3rd" : "member",
    });
  }
  return memberLinks;
}

async function parseMember(link, fetchedAt) {
  const html = await fetchText(link.url);
  const fieldBlock =
    html.match(/<div class="artist_detail_profile[\s\S]*?<dl>([\s\S]*?)<\/dl>/i)?.[1] ??
    html;
  const fields = parseDl(fieldBlock);
  const characterHref = attrFirst(
    html,
    /artist_detail_textarea_headchara[\s\S]*?<a\s+href="([^"]+)"/i,
  );
  const appearanceBlock =
    html.match(/APPEARANCE WORK[\s\S]*?<dl>([\s\S]*?)<\/dl>/i)?.[1] ?? "";
  const appearanceWork = parseDl(appearanceBlock);
  const sourceUrl = link.url;

  return {
    id: link.code,
    official_section: link.official_section,
    names: {
      ja: firstMatch(html, /<h2 class="artist_detail_name">([\s\S]*?)<\/h2>/i),
      kana: firstMatch(html, /<div class="artist_detail_furi">([\s\S]*?)<\/div>/i),
    },
    profile: {
      birthplace: fields["出身地"] || null,
      birthday: {
        label: fields["誕生日"] || null,
        month_day: fields["誕生日"] ? parseMonthDay(fields["誕生日"]) : null,
      },
      blood_type: fields["血液型"] || null,
      zodiac: fields["星座"] || null,
      member_color: fields["メンバーカラー"] || null,
      height_cm: fields["身長"] ? parseHeightCm(fields["身長"]) : null,
      hobbies: fields["趣味"] || null,
      skills: fields["特技"] || null,
      comment: fields["ひとこと"] || null,
    },
    character: {
      source_url: characterHref ? canonicalCharacterUrl(characterHref) : null,
      name_from_appearance_work: appearanceWork["22/7"] || null,
    },
    appearance_work: Object.keys(appearanceWork).length ? appearanceWork : null,
    sns: parseSns(html),
    media: {
      image_url: absoluteUrl(
        attrFirst(html, /<div class="artist_detail_thumb">\s*<img\s+src="([^"]+)"/i),
      ),
    },
    sources: [
      {
        id: "22_7_official_member_profile",
        type: "official_page",
        url: sourceUrl,
        fetched_at: fetchedAt,
      },
    ],
    verification_status: "official_profile",
  };
}

async function parseCharacter(url, fetchedAt, voiceActorByCharacterUrl) {
  const html = await fetchText(url);
  const profileBlock =
    html.match(/<div class="character_detail_profile pconly">([\s\S]*?)<\/div>\s*<\/div>\s*<\/div>/i)
      ?.[1] ?? html;
  const fields = parseDl(profileBlock);
  const code = new URL(url).pathname.match(/\/artist\/(c\d+)/)?.[1] ?? "";

  return {
    id: code,
    names: {
      ja: firstMatch(html, /<div class="character_name_wrap[^"]*">\s*<h3>([\s\S]*?)<\/h3>/i),
      romaji: firstMatch(
        html,
        /<span class="character_detail_title_en webfont">([\s\S]*?)<\/span>/i,
      ),
    },
    voice_actor_member: voiceActorByCharacterUrl.get(url) || null,
    profile: {
      birthday: {
        label: fields["誕生日"] || null,
        month_day: fields["誕生日"] ? parseMonthDay(fields["誕生日"]) : null,
      },
      age: fields["年齢"] || null,
      blood_type: fields["血液型"] || null,
      zodiac: fields["星座"] || null,
      height_cm: fields["身長"] ? parseHeightCm(fields["身長"]) : null,
      birthplace: fields["出身地"] || null,
      penlight_color: fields["ペンライト"] || null,
      motto: fields["座右の銘"] || null,
      skills: fields["特技"] || null,
      dream: fields["将来の夢"] || null,
      likes: fields["好きなもの"] || null,
      dislikes: fields["苦手なもの"] || null,
      hobbies: fields["趣味"] || null,
    },
    media: {
      image_url: absoluteUrl(
        attrFirst(html, /character_detail_img_main1">\s*<img\s+src="([^"]+)"/i),
      ),
    },
    sources: [
      {
        id: "22_7_official_character_profile",
        type: "official_page",
        url,
        fetched_at: fetchedAt,
      },
    ],
    verification_status: "official_profile",
  };
}

function stripJsonp(raw) {
  return raw.replace(/^[^(]+\(/, "").replace(/\);?\s*$/, "");
}

function normalizeSonyCode(code) {
  return code.replace(/[^0-9A-Za-z]/g, "").toLowerCase();
}

async function fetchDiscographyList() {
  const count = 100;
  const items = [];
  for (let start = 0; ; start += count) {
    const url = SONY_DISCO_LIST.replace("{start}", String(start)).replace(
      "{count}",
      String(count),
    );
    const page = JSON.parse(stripJsonp(await fetchText(url))).items ?? [];
    items.push(...page);
    if (page.length < count) break;
  }
  return items;
}

function stripHtmlText(value) {
  return stripTags(String(value ?? ""));
}

function sonyPageUrl(code) {
  return `https://www.sonymusic.co.jp/artist/nanabunnonijyuuni/discography/${code}`;
}

function isoDate(value) {
  const match = String(value ?? "").match(/(\d{4})\.(\d{2})\.(\d{2})/);
  return match ? `${match[1]}-${match[2]}-${match[3]}` : null;
}

function baseReleaseTitle(title) {
  return String(title ?? "")
    .replace(/【[^】]+】/g, "")
    .replace(/\s+/g, " ")
    .trim();
}

function flattenTracks(discs = []) {
  const tracks = [];
  for (const disc of discs) {
    for (const content of disc.contents ?? []) {
      tracks.push({
        disc_number: disc.disc_number ?? null,
        track_number: content.track_number ?? null,
        title: stripHtmlText(content.title),
      });
    }
  }
  return tracks;
}

async function parseDiscography(fetchedAt) {
  const indexItems = await fetchDiscographyList();
  const editions = [];
  for (const item of indexItems) {
    const code = item.representative_goods_number;
    const callback = normalizeSonyCode(code);
    const detailUrl = SONY_DISCO_DETAIL.replace("{code}", encodeURIComponent(code)).replace(
      "{callback}",
      callback,
    );
    const detail = JSON.parse(stripJsonp(await fetchText(detailUrl))).items ?? item;
    editions.push({
      id: code,
      title: detail.title,
      base_title: baseReleaseTitle(detail.title),
      type: detail.type,
      release_date: isoDate(detail.release_date),
      display_release_date: detail.display_release_date || detail.release_date || null,
      catalog_number: detail.representative_goods_number || code,
      display_catalog_number: detail.display_goods_number || null,
      price: detail.price || null,
      catch_copy: stripHtmlText(detail.catch_copy),
      tracks: flattenTracks(detail.discs),
      source_url: sonyPageUrl(code),
      api_url: detailUrl,
      sources: [
        {
          id: "sony_music_discography_api",
          type: "official_jsonp_api",
          url: detailUrl,
          fetched_at: fetchedAt,
        },
      ],
    });
  }

  const groupMap = new Map();
  for (const edition of editions) {
    const key = `${edition.base_title}::${edition.release_date}::${edition.type}`;
    if (!groupMap.has(key)) {
      groupMap.set(key, {
        id: slugify(`${edition.base_title}-${edition.release_date}-${edition.type}`),
        title: edition.base_title,
        type: edition.type,
        release_date: edition.release_date,
        edition_ids: [],
        catalog_numbers: [],
        track_titles: [],
        source_urls: [],
      });
    }
    const group = groupMap.get(key);
    group.edition_ids.push(edition.id);
    group.catalog_numbers.push(edition.display_catalog_number || edition.catalog_number);
    group.source_urls.push(edition.source_url);
    for (const track of edition.tracks) {
      if (track.title && !group.track_titles.includes(track.title)) {
        group.track_titles.push(track.title);
      }
    }
  }

  const songMap = new Map();
  for (const edition of editions) {
    for (const track of edition.tracks) {
      if (!track.title) continue;
      const key = track.title;
      const known = songMap.get(key);
      if (!known || String(edition.release_date) < String(known.first_release_date)) {
        songMap.set(key, {
          title: key,
          first_release_date: edition.release_date,
          first_release: edition.base_title,
          first_edition_id: edition.id,
          source_url: edition.source_url,
        });
      }
    }
  }

  return {
    editions,
    release_groups: [...groupMap.values()].sort(compareByDateDescThenTitle),
    songs: [...songMap.values()].sort(compareByDateDescThenTitle),
  };
}

function normalizeDottedDate(value) {
  const match = String(value ?? "").match(/(\d{4})\.(\d{1,2})\.(\d{1,2})/);
  if (!match) return null;
  return `${match[1]}-${match[2].padStart(2, "0")}-${match[3].padStart(2, "0")}`;
}

async function parseOfficialLiveReports(fetchedAt) {
  const reports = [];
  const seen = new Set();
  let url = OFFICIAL_LIVE_REPORT_LIST;

  for (let page = 0; page < 20 && url; page += 1) {
    const html = await fetchText(url);
    const itemPattern =
      /<div class="special_box[\s\S]*?<div class="special_thumb">[\s\S]*?<a\s+href="([^"]+)"[\s\S]*?<div class="special_date[^"]*">([\s\S]*?)<\/div>\s*<div class="special_title">([\s\S]*?)<\/div>/gi;
    for (const match of html.matchAll(itemPattern)) {
      const sourceUrl = absoluteUrl(match[1]);
      if (seen.has(sourceUrl)) continue;
      seen.add(sourceUrl);
      reports.push({
        id: `live-report-${sourceUrl.match(/detail\/(\d+)/)?.[1] ?? slugify(sourceUrl)}`,
        title: stripTags(match[3]).replace(/\s+/g, " "),
        date: normalizeDottedDate(stripTags(match[2])),
        date_kind: "report_date",
        date_precision: "day",
        source_url: sourceUrl,
        sources: [
          {
            id: "22_7_official_live_report_list",
            type: "official_live_report",
            url: sourceUrl,
            fetched_at: fetchedAt,
          },
        ],
      });
    }

    const nextHref = attrFirst(
      html,
      /<li class="next">\s*<a\s+href="([^"]+)">次のページ<\/a>\s*<\/li>/i,
    );
    url = nextHref ? absoluteUrl(nextHref) : "";
  }

  return reports.sort(compareByDateDescThenTitle);
}

function buildPerformanceFacts(generatedAt, liveReports) {
  const confirmedEvents = OFFICIAL_PERFORMANCE_EVENTS.map((event) => ({
    ...event,
    sources: event.sources.map((source) => ({ ...source, fetched_at: generatedAt })),
  })).sort(compareByDateAscThenTitle);

  return {
    confirmed_events: confirmedEvents,
    official_live_reports: liveReports,
  };
}

function compareByDateDescThenTitle(a, b) {
  const ad = a.date ?? a.release_date ?? a.first_release_date ?? "";
  const bd = b.date ?? b.release_date ?? b.first_release_date ?? "";
  if (ad !== bd) return ad < bd ? 1 : -1;
  return (a.title ?? "").localeCompare(b.title ?? "", "ja");
}

function compareByDateAscThenTitle(a, b) {
  const ad = a.date ?? a.release_date ?? "";
  const bd = b.date ?? b.release_date ?? "";
  if (ad !== bd) return ad < bd ? -1 : 1;
  return (a.title ?? "").localeCompare(b.title ?? "", "ja");
}

function slugify(value) {
  return value
    .normalize("NFKC")
    .replace(/[^\p{L}\p{N}]+/gu, "-")
    .replace(/^-+|-+$/g, "")
    .toLowerCase();
}

function metadata(kind, generatedAt, sources) {
  return {
    schema_version: 1,
    kind,
    generated_at: generatedAt,
    source_policy:
      "Official 22/7 mobile pages and Sony Music official JSONP/API pages are authoritative for stored facts. Secondary search results are used only for discovery/cross-check notes unless explicitly promoted later.",
    sources,
  };
}

async function main() {
  const generatedAt = new Date().toISOString();
  const seedHtml = await fetchText(OFFICIAL_MEMBER_SEED);
  const memberLinks = parseMemberIndex(seedHtml);
  const members = [];
  for (const link of memberLinks) {
    members.push(await parseMember(link, generatedAt));
  }

  const voiceActorByCharacterUrl = new Map();
  const characterUrls = [];
  for (const member of members) {
    if (member.character.source_url) {
      characterUrls.push(member.character.source_url);
      voiceActorByCharacterUrl.set(member.character.source_url, {
        id: member.id,
        name: member.names.ja,
      });
    }
  }

  const characters = [];
  for (const url of [...new Set(characterUrls)]) {
    characters.push(await parseCharacter(url, generatedAt, voiceActorByCharacterUrl));
  }

  const discography = await parseDiscography(generatedAt);
  const liveReports = await parseOfficialLiveReports(generatedAt);
  const performanceFacts = buildPerformanceFacts(generatedAt, liveReports);

  await mkdir(OUTPUT_DIR, { recursive: true });
  await writeJson("members.json", {
    ...metadata("22_7_members", generatedAt, [
      {
        id: "22_7_official_member_seed",
        type: "official_page",
        url: OFFICIAL_MEMBER_SEED,
        fetched_at: generatedAt,
      },
    ]),
    count: members.length,
    items: members,
  });
  await writeJson("characters.json", {
    ...metadata("22_7_characters", generatedAt, [
      {
        id: "22_7_official_member_seed",
        type: "official_page",
        url: OFFICIAL_MEMBER_SEED,
        fetched_at: generatedAt,
      },
    ]),
    count: characters.length,
    items: characters,
  });
  await writeJson("discography.json", {
    ...metadata("22_7_discography", generatedAt, [
      {
        id: "sony_music_discography_index_api",
        type: "official_jsonp_api",
        url: SONY_DISCO_LIST.replace("{start}", "0").replace("{count}", "100"),
        fetched_at: generatedAt,
      },
      {
        id: "sony_music_discography_human_page",
        type: "official_page",
        url: "https://www.sonymusic.co.jp/artist/nanabunnonijyuuni/discography/",
        fetched_at: generatedAt,
      },
    ]),
    counts: {
      editions: discography.editions.length,
      release_groups: discography.release_groups.length,
      songs: discography.songs.length,
    },
    editions: discography.editions,
    release_groups: discography.release_groups,
    songs: discography.songs,
  });
  await writeJson("performances.json", {
    ...metadata("22_7_performances", generatedAt, [
      {
        id: "22_7_official_live_report_list",
        type: "official_live_report_list",
        url: OFFICIAL_LIVE_REPORT_LIST,
        fetched_at: generatedAt,
      },
      ...collectUniqueSources(performanceFacts.confirmed_events, generatedAt),
    ]),
    counts: {
      confirmed_events: performanceFacts.confirmed_events.length,
      official_live_reports: performanceFacts.official_live_reports.length,
    },
    ...performanceFacts,
  });
  await writeJson("source-index.json", {
    ...metadata("22_7_source_index", generatedAt, [
      {
        id: "22_7_official_member_seed",
        type: "official_page",
        url: OFFICIAL_MEMBER_SEED,
        fetched_at: generatedAt,
      },
      {
        id: "sony_music_discography_index_api",
        type: "official_jsonp_api",
        url: SONY_DISCO_LIST.replace("{start}", "0").replace("{count}", "100"),
        fetched_at: generatedAt,
      },
      {
        id: "sony_music_discography_human_page",
        type: "official_page",
        url: "https://www.sonymusic.co.jp/artist/nanabunnonijyuuni/discography/",
        fetched_at: generatedAt,
      },
      {
        id: "22_7_official_live_report_list",
        type: "official_live_report_list",
        url: OFFICIAL_LIVE_REPORT_LIST,
        fetched_at: generatedAt,
      },
      ...collectUniqueSources(performanceFacts.confirmed_events, generatedAt),
      {
        id: "web_search_discovery",
        type: "search_discovery_note",
        url: "https://www.google.com/search?q=22%2F7+%E5%85%AC%E5%BC%8F+%E3%83%97%E3%83%AD%E3%83%95%E3%82%A3%E3%83%BC%E3%83%AB+%E3%83%A1%E3%83%B3%E3%83%90%E3%83%BC+%E8%AA%95%E7%94%9F%E6%97%A5",
        fetched_at: generatedAt,
        note: "Search was used to discover the official mobile member profile URL and to identify secondary sources; secondary sources are not imported as authoritative facts.",
      },
    ]),
  });
  await writeText(
    "prompt-facts-brief.txt",
    renderPromptFactsBrief(
      generatedAt,
      members,
      characters,
      discography.release_groups,
      performanceFacts.confirmed_events,
    ),
  );
  await writeText(
    "song-title-index.txt",
    renderSongTitleIndex(generatedAt, discography.songs),
  );
  await writeText(
    "release-calendar.txt",
    renderReleaseCalendar(generatedAt, discography.release_groups),
  );
  await writeText(
    "performance-calendar.txt",
    renderPerformanceCalendar(generatedAt, performanceFacts),
  );
  await writeText(
    "prompt-knowledge-compact.txt",
    renderCompactPromptKnowledge(
      generatedAt,
      members,
      characters,
      discography.release_groups,
      performanceFacts.confirmed_events,
    ),
  );

  console.log(
    `Wrote ${members.length} members, ${characters.length} characters, ${discography.editions.length} discography editions, ${performanceFacts.confirmed_events.length} performance events, ${performanceFacts.official_live_reports.length} live reports.`,
  );
}

function collectUniqueSources(items, fetchedAt) {
  const sources = [];
  const seen = new Set();
  for (const item of items) {
    for (const source of item.sources ?? []) {
      const key = `${source.id}:${source.url}`;
      if (seen.has(key)) continue;
      seen.add(key);
      sources.push({ ...source, fetched_at: fetchedAt });
    }
  }
  return sources;
}

async function writeJson(name, data) {
  const file = new URL(name, OUTPUT_DIR);
  await writeFile(file, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}

async function writeText(name, text) {
  const file = new URL(name, OUTPUT_DIR);
  await writeFile(file, text.endsWith("\n") ? text : `${text}\n`, "utf8");
}

function renderPromptFactsBrief(generatedAt, members, characters, releaseGroups, performanceEvents) {
  const memberLines = members.map((member) => {
    const profile = member.profile;
    const character =
      member.character.name_from_appearance_work ||
      characters.find((item) => item.voice_actor_member?.id === member.id)?.names.ja ||
      "未割当/未収録";
    return [
      `${member.names.ja}（${member.names.kana}）`,
      `誕生日:${profile.birthday.label}`,
      `メンバーカラー:${profile.member_color}`,
      `出身地:${profile.birthplace}`,
      `身長:${profile.height_cm ?? "不明"}cm`,
      `22/7キャラクター:${character}`,
    ].join(" | ");
  });

  const characterLines = characters.map((character) =>
    [
      `${character.names.ja}（${character.names.romaji}）`,
      `CV:${character.voice_actor_member?.name ?? "不明"}`,
      `誕生日:${character.profile.birthday.label}`,
      `ペンライト:${character.profile.penlight_color}`,
    ].join(" | "),
  );

  const releaseLines = releaseGroups
    .filter((release) => ["シングル", "アルバム"].includes(release.type))
    .sort(compareByDateAscThenTitle)
    .map((release) =>
      [
        release.release_date,
        release.type,
        release.title,
        `editions:${release.edition_ids.length}`,
      ].join(" | "),
    );

  const performanceLines = performanceEvents
    .filter((event) => ["performance_date", "event_date", "stream_date"].includes(event.date_kind))
    .map((event) =>
      [
        event.date,
        event.title,
        `period:${event.period}`,
        event.venue ? `venue:${event.venue}` : null,
        event.lineup_note ? `note:${event.lineup_note}` : null,
      ]
        .filter(Boolean)
        .join(" | "),
    );

  return `# 22/7 Prompt Facts Brief

Generated: ${generatedAt}
Authority: official 22/7 mobile pages and Sony Music official discography API. Use this as factual lookup context; do not invent missing facts. Performance entries intentionally keep date-level facts only; detailed open/start/end times are omitted.

## Members
${memberLines.join("\n")}

## Characters
${characterLines.join("\n")}

## Singles And Albums
${releaseLines.join("\n")}

## Performance Dates
${performanceLines.join("\n")}
`;
}

function renderSongTitleIndex(generatedAt, songs) {
  const lines = songs.map((song) =>
    [
      song.title,
      `first_release_date:${song.first_release_date}`,
      `first_release:${song.first_release}`,
      `source_edition:${song.first_edition_id}`,
    ].join(" | "),
  );

  return `# 22/7 Official Track Title Index

Generated: ${generatedAt}
Authority: Sony Music official discography API. Titles are source titles, not Chinese translations.

${lines.join("\n")}
`;
}

function renderCompactPromptKnowledge(generatedAt, members, characters, releaseGroups, performanceEvents) {
  const memberLines = members.map((member) => {
    const character =
      member.character.name_from_appearance_work ||
      characters.find((item) => item.voice_actor_member?.id === member.id)?.names.ja ||
      "";
    return [
      member.names.ja,
      member.names.kana,
      `birthday:${member.profile.birthday.label}`,
      `color:${member.profile.member_color}`,
      character ? `character:${character}` : null,
    ]
      .filter(Boolean)
      .join(" / ");
  });

  const characterLines = characters.map((character) =>
    [
      character.names.ja,
      character.names.romaji,
      `CV:${character.voice_actor_member?.name ?? "unknown"}`,
      `birthday:${character.profile.birthday.label}`,
      `penlight:${character.profile.penlight_color}`,
    ].join(" / "),
  );

  const releaseLines = releaseGroups
    .filter((release) => ["シングル", "アルバム"].includes(release.type))
    .sort(compareByDateAscThenTitle)
    .map((release) => `${release.release_date} ${release.title} (${release.type})`);

  const performanceLines = performanceEvents.map((event) =>
    [
      `${event.date} ${event.title}`,
      event.venue ? `venue:${event.venue}` : null,
      event.lineup_note ? `note:${event.lineup_note}` : null,
    ]
      .filter(Boolean)
      .join(" / "),
  );

  return `# 22/7 Compact Official Facts For JP-ZH Translation

Generated: ${generatedAt}
Use only to stabilize member names, character names, titles, release dates, and performance dates when the source text mentions them. Do not insert facts absent from the source. Performance entries are day-level facts only; detailed open/start/end times are intentionally omitted.

## Members
${memberLines.join("\n")}

## Characters
${characterLines.join("\n")}

## Release Dates
${releaseLines.join("\n")}

## Performance Dates
${performanceLines.join("\n")}
`;
}

function renderReleaseCalendar(generatedAt, releaseGroups) {
  const lines = releaseGroups
    .filter((release) => release.release_date)
    .sort(compareByDateAscThenTitle)
    .map((release) =>
      [
        release.release_date,
        release.type,
        release.title,
        `editions:${release.edition_ids.length}`,
      ].join(" | "),
    );

  return `# 22/7 Official Release Calendar

Generated: ${generatedAt}
Authority: Sony Music official discography API. Dates are release dates, not announcement dates.

${lines.join("\n")}
`;
}

function renderPerformanceCalendar(generatedAt, performanceFacts) {
  const confirmedLines = performanceFacts.confirmed_events.map((event) =>
    [
      event.date,
      event.title,
      `kind:${event.kind}`,
      `period:${event.period}`,
      `date_kind:${event.date_kind}`,
      event.venue ? `venue:${event.venue}` : null,
      event.lineup_note ? `note:${event.lineup_note}` : null,
    ]
      .filter(Boolean)
      .join(" | "),
  );

  const reportLines = performanceFacts.official_live_reports.map((report) =>
    [report.date, report.title, `date_kind:${report.date_kind}`, `source:${report.source_url}`].join(
      " | ",
    ),
  );

  return `# 22/7 Official Performance Calendar

Generated: ${generatedAt}
Authority: 22/7 official schedule/news/live report pages. Confirmed events store day-level event dates; live reports store report dates unless the event date is confirmed separately.

## Confirmed Date Events
${confirmedLines.join("\n")}

## Official Live Report References
${reportLines.join("\n")}
`;
}

await main();
