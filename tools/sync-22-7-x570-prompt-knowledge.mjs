#!/usr/bin/env node

import { readFile, writeFile } from "node:fs/promises";

const REPO_ROOT = new URL("../", import.meta.url);
const WORKSPACE_ROOT = new URL("../..", import.meta.url);
const FACTS_DIR = new URL("assets/knowledge/22_7/facts/", REPO_ROOT);
const TRANSLATION_DIR = new URL("assets/knowledge/22_7/translation/", REPO_ROOT);
const SUITE_DIR = new URL("amt_asr_suite/", WORKSPACE_ROOT);

const FACTS_BEGIN = "=== BEGIN 22/7 OFFICIAL FACTS FOR JP-ZH ===";
const FACTS_END = "=== END 22/7 OFFICIAL FACTS FOR JP-ZH ===";
const RULE_BEGIN = "=== BEGIN 22/7 FACTUAL REFERENCE RULE ===";
const RULE_END = "=== END 22/7 FACTUAL REFERENCE RULE ===";

const targetOverrides = new Map([
  ["22/7", { target: "22/7", note: "group name" }],
  ["ナナニジ", { target: "22/7", note: "group nickname" }],
  ["ナナブンノニジュウニ", { target: "22/7", note: "group reading" }],
  ["二十二分の七", { target: "22/7", note: "group name" }],
  ["7分の22", { target: "22/7", note: "common ASR confusion" }],
  ["七日", { target: "22/7", note: "common ASR confusion when context is Nananiji" }],
  ["七虹", { target: "22/7", note: "Chinese operator nickname for 22/7" }],
  ["計算中", { target: "计算中", note: "program title suffix" }],
  ["検算中", { target: "验算中", note: "program title suffix" }],
  ["計算外", { target: "计算外", note: "program title suffix" }],
  ["22/7計算外", { target: "22/7计算外", note: "variety program title" }],
  ["ナナコミ", { target: "NANA-COMI", note: "official BBS name" }],
  ["ナナニジハウス", { target: "七虹家", note: "22/7 fanclub name" }],
  ["オーディー", { target: "Audee", note: "broadcast provider" }],
  ["Audee", { target: "Audee", note: "broadcast provider" }],
  ["うたっけ", { target: "诗家", note: "Kawase Uta membership program" }],
  ["二つの道", { target: "二つの道", note: "16th single title; keep Japanese title unless official Chinese title is decided" }],
  ["蝉は夏を知らない", { target: "蝉は夏を知らない", note: "song title; keep Japanese title unless official Chinese title is decided" }],
  ["命の続き", { target: "命の続き", note: "song title; keep Japanese title unless official Chinese title is decided" }],
  ["未来があるから", { target: "未来があるから", note: "song title; keep Japanese title unless official Chinese title is decided" }],
  ["氷室みず姫", { target: "冰室水姬", note: "character name" }],
  ["Fivesta", { target: "Fivesta", note: "22/7 live title" }],
  ["ナナニジライブ2026", { target: "ナナニジライブ2026", note: "3rd generation regular live title" }],
  ["22/7 LIVE「15」", { target: "22/7 LIVE「15」", note: "2026 live title" }],
  ["22/7 Live in Taipei 2026", { target: "22/7 Live in Taipei 2026", note: "2026 Taipei live title" }],
  ["22/7 ANNIVERSARY LIVE 2026", { target: "22/7 ANNIVERSARY LIVE 2026", note: "2026 anniversary live title" }],
]);

const legacyMemberPriority = [
  "西條和",
  "涼花萌",
  "宮瀬玲奈",
  "佐倉初",
  "白沢かなえ",
  "四条月",
  "花宮初奈",
];

const legacyCharacterPriority = [
  "滝川みう",
  "神木みかみ",
  "八神叶愛",
  "永峰楓",
  "一之瀬蛍",
];

const titlePriority = [
  "計算中",
  "検算中",
  "計算外",
  "22/7計算外",
  "ナナコミ",
  "ナナニジハウス",
  "Audee",
  "オーディー",
  "うたっけ",
  "二つの道",
  "蝉は夏を知らない",
  "命の続き",
  "未来があるから",
  "Fivesta",
  "ナナニジライブ2026",
  "22/7 LIVE「15」",
  "22/7 Live in Taipei 2026",
  "22/7 ANNIVERSARY LIVE 2026",
];

async function readText(url) {
  return readFile(url, "utf8");
}

async function readJson(url) {
  return JSON.parse(await readText(url));
}

async function writeText(url, text) {
  await writeFile(url, text.endsWith("\n") ? text : `${text}\n`, "utf8");
}

function replaceOrInsertBlock(text, begin, end, block, anchor, placement = "before") {
  const escapedBegin = escapeRegExp(begin);
  const escapedEnd = escapeRegExp(end);
  const existing = new RegExp(`${escapedBegin}[\\s\\S]*?${escapedEnd}`, "m");
  if (existing.test(text)) {
    return text.replace(existing, block);
  }

  const index = text.indexOf(anchor);
  if (index < 0) {
    throw new Error(`Anchor not found: ${anchor}`);
  }
  if (placement === "after") {
    const insertAt = index + anchor.length;
    return `${text.slice(0, insertAt)}\n\n${block}\n\n${text.slice(insertAt)}`;
  }
  return `${text.slice(0, index)}${block}\n\n${text.slice(index)}`;
}

function escapeRegExp(value) {
  return value.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

function renderFactsBlock(compactFacts) {
  return `${FACTS_BEGIN}
${compactFacts.trim()}
${FACTS_END}`;
}

function renderRuleBlock() {
  return `${RULE_BEGIN}
If an official 22/7 factual reference block is present in the prompt or cold context, use it only to stabilize names, titles, release dates, and performance dates that are mentioned in the source text. Never insert absent facts, never infer ticket/time details, and keep performance facts at date level unless the source itself states more.
${RULE_END}`;
}

function oldTermLookup(oldTerms) {
  const map = new Map();
  for (const term of oldTerms) {
    if (!map.has(term.source)) {
      map.set(term.source, term);
    }
  }
  return map;
}

function makeTerm(source, lookup, noteFallback = "") {
  const override = targetOverrides.get(source);
  if (override) {
    return { source, target: override.target, note: override.note };
  }
  const known = lookup.get(source);
  if (known) {
    return known;
  }
  return { source, target: source, note: noteFallback };
}

function pushUniqueTerm(output, term) {
  if (!term?.source || output.some((item) => item.source === term.source)) {
    return;
  }
  output.push(term);
}

function buildPrioritizedTerms({ members, characters, oldTerms }) {
  const lookup = oldTermLookup(oldTerms);
  const output = [];
  for (const source of ["22/7", "ナナニジ", "ナナブンノニジュウニ", "二十二分の七", "7分の22", "七日", "七虹"]) {
    pushUniqueTerm(output, makeTerm(source, lookup));
  }

  for (const member of members.items) {
    pushUniqueTerm(output, makeTerm(member.names.ja, lookup, "current member name"));
  }
  for (const source of legacyMemberPriority) {
    pushUniqueTerm(output, makeTerm(source, lookup, "legacy member name"));
  }

  for (const character of characters.items) {
    pushUniqueTerm(output, makeTerm(character.names.ja, lookup, "character name"));
  }
  for (const source of legacyCharacterPriority) {
    pushUniqueTerm(output, makeTerm(source, lookup, "character name"));
  }

  for (const source of titlePriority) {
    pushUniqueTerm(output, makeTerm(source, lookup, "22/7 title or domain term"));
  }

  for (const term of oldTerms) {
    pushUniqueTerm(output, term);
  }
  return output;
}

async function syncPromptFiles(compactFacts) {
  const factsBlock = renderFactsBlock(compactFacts);
  const ruleBlock = renderRuleBlock();

  const sprtPath = new URL("rough_srt_processor/sprtT.txt", SUITE_DIR);
  const sprt = await readText(sprtPath);
  await writeText(
    sprtPath,
    replaceOrInsertBlock(
      sprt,
      FACTS_BEGIN,
      FACTS_END,
      factsBlock,
      "Static style activation samples:",
      "before",
    ),
  );

  const livePromptPath = new URL("windows/ds_v4_flash_translation_prompt_ja_zh.txt", SUITE_DIR);
  const livePrompt = await readText(livePromptPath);
  await writeText(
    livePromptPath,
    replaceOrInsertBlock(
      livePrompt,
      RULE_BEGIN,
      RULE_END,
      ruleBlock,
      "Unrelated cold text for MOE routing activation only:",
      "before",
    ),
  );

  const liveColdPath = new URL("windows/ds_v4_flash_cold_context_22_7.txt", SUITE_DIR);
  const liveCold = await readText(liveColdPath);
  await writeText(
    liveColdPath,
    replaceOrInsertBlock(
      liveCold,
      FACTS_BEGIN,
      FACTS_END,
      factsBlock,
      "The raw Japanese text below was supplied by the operator for routing activation.",
      "before",
    ),
  );
}

async function syncTerms() {
  const members = await readJson(new URL("members.json", FACTS_DIR));
  const characters = await readJson(new URL("characters.json", FACTS_DIR));
  const termsPath = new URL("translation_terms_22_7_ja_zh.json", TRANSLATION_DIR);
  const oldTerms = await readJson(termsPath);
  const terms = buildPrioritizedTerms({ members, characters, oldTerms });
  const rendered = `${JSON.stringify(terms, null, 2)}\n`;
  await writeText(termsPath, rendered);
  await writeText(new URL("windows/translation_terms_22_7_ja_zh.json", SUITE_DIR), rendered);
  return terms.length;
}

async function main() {
  const compactFacts = await readText(new URL("prompt-knowledge-compact.txt", FACTS_DIR));
  await syncPromptFiles(compactFacts);
  const termCount = await syncTerms();
  console.log(`Synced 22/7 prompt facts and ${termCount} prioritized terms.`);
}

await main();
