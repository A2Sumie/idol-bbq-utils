#!/usr/bin/env node
import { spawn } from 'node:child_process';
import { mkdtempSync, readFileSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

const DEFAULT_TARGET = 'https://x.com/rino_mochizuki';
const DEFAULT_COOKIE_FILES = [
  '/app/assets/cookies/xcookies.txt',
  'assets/cookies/xcookies.txt',
];

const args = parseArgs(process.argv.slice(2));
const targetUrl = args.target || process.env.X_GROK_TARGET || DEFAULT_TARGET;
const cookieFile =
  args.cookies ||
  process.env.X_GROK_COOKIE_FILE ||
  DEFAULT_COOKIE_FILES.find((file) => fileExists(file));
const chromePath =
  args.chrome ||
  process.env.PUPPETEER_EXECUTABLE_PATH ||
  process.env.CHROME_BIN ||
  '/usr/bin/google-chrome-stable';
const dstLang = args.lang || process.env.X_GROK_DST_LANG || 'zh';
const waitMs = Number(args.waitMs || process.env.X_GROK_WAIT_MS || 5000);

const userDataDir = mkdtempSync(path.join(tmpdir(), 'x-grok-probe-'));
let chrome;

try {
  const cookies = cookieFile ? parseNetscapeCookies(readFileSync(cookieFile, 'utf8')) : [];
  chrome = launchChrome(chromePath, userDataDir);
  const { port } = await waitForDevToolsPort(userDataDir);
  const pageWsUrl = await getPageWsUrl(port);
  const cdp = await CdpClient.connect(pageWsUrl);

  const interesting = [];
  const requests = new Map();

  cdp.on('Network.requestWillBeSent', (event) => {
    if (!event.request?.url?.includes('/2/grok/translation.json')) return;
    requests.set(event.requestId, event.request.url);
    interesting.push({
      kind: 'request',
      method: event.request.method,
      url: event.request.url,
      headers: redactHeaders(event.request.headers),
      postData: safeJson(event.request.postData),
    });
  });
  cdp.on('Network.responseReceived', (event) => {
    if (!event.response?.url?.includes('/2/grok/translation.json')) return;
    requests.set(event.requestId, event.response.url);
    interesting.push({
      kind: 'response',
      status: event.response.status,
      url: event.response.url,
      headers: redactHeaders(event.response.headers),
    });
  });
  cdp.on('Network.loadingFinished', async (event) => {
    const url = requests.get(event.requestId);
    if (!url) return;
    try {
      const body = await cdp.send('Network.getResponseBody', { requestId: event.requestId });
      const decodedBody = body.base64Encoded
        ? Buffer.from(body.body, 'base64').toString('utf8')
        : body.body;
      interesting.push({
        kind: 'body',
        url,
        base64Encoded: body.base64Encoded,
        body: redactText(decodedBody).slice(0, 2000),
      });
    } catch {
      interesting.push({ kind: 'body-unavailable', url });
    }
  });

  await cdp.send('Network.enable');
  await cdp.send('Page.enable');
  await cdp.send('Runtime.enable');
  await cdp.send('Network.setExtraHTTPHeaders', {
    headers: {
      'accept-language': 'zh-CN,zh;q=0.9,ja;q=0.8,en;q=0.7',
      'x-twitter-client-language': dstLang.toLowerCase(),
    },
  });
  if (cookies.length > 0) {
    await cdp.send('Network.setCookies', { cookies });
  }

  await cdp.send('Page.navigate', { url: targetUrl });
  const clickResult = await waitForTranslateControl(cdp, waitMs * 2);
  if (clickResult?.found && Number.isFinite(clickResult.x) && Number.isFinite(clickResult.y)) {
    await cdp.send('Input.dispatchMouseEvent', {
      type: 'mouseMoved',
      x: clickResult.x,
      y: clickResult.y,
    });
    await cdp.send('Input.dispatchMouseEvent', {
      type: 'mousePressed',
      x: clickResult.x,
      y: clickResult.y,
      button: 'left',
      clickCount: 1,
    });
    await cdp.send('Input.dispatchMouseEvent', {
      type: 'mouseReleased',
      x: clickResult.x,
      y: clickResult.y,
      button: 'left',
      clickCount: 1,
    });
  }
  await delay(waitMs);
  const pageSummary = await cdp.eval(pageSummaryExpression());

  console.log(
    JSON.stringify(
      {
        targetUrl,
        cookieFile: cookieFile ? redactPath(cookieFile) : null,
        chromePath,
        clicked: clickResult,
        pageSummary,
        interesting,
      },
      null,
      2,
    ),
  );

  await cdp.close();
} finally {
  if (chrome && !chrome.killed) chrome.kill('SIGTERM');
  rmSync(userDataDir, { recursive: true, force: true });
}

async function waitForTranslateControl(cdp, timeoutMs) {
  const startedAt = Date.now();
  let lastResult = null;
  while (Date.now() - startedAt < timeoutMs) {
    lastResult = await cdp.eval(findTranslateControlExpression());
    if (lastResult?.found) return lastResult;
    await delay(500);
  }
  return lastResult;
}

function parseArgs(argv) {
  const result = {};
  for (let index = 0; index < argv.length; index += 1) {
    const arg = argv[index];
    if (!arg.startsWith('--')) continue;
    const key = arg.slice(2);
    const next = argv[index + 1];
    if (!next || next.startsWith('--')) {
      result[key] = '1';
    } else {
      result[key] = next;
      index += 1;
    }
  }
  return result;
}

function fileExists(file) {
  try {
    readFileSync(file);
    return true;
  } catch {
    return false;
  }
}

function parseNetscapeCookies(text) {
  const cookies = [];
  for (const line of text.split(/\r?\n/)) {
    if (!line || line.startsWith('#')) continue;
    const parts = line.split('\t');
    if (parts.length < 7) continue;
    const [domain, , cookiePath, secure, expires, name, value] = parts;
    cookies.push({
      name,
      value,
      domain,
      path: cookiePath || '/',
      secure: secure === 'TRUE',
      expirationDate: Number(expires) || undefined,
    });
  }
  return cookies;
}

function launchChrome(executablePath, userDataDir) {
  return spawn(
    executablePath,
    [
      '--headless=new',
      '--no-sandbox',
      '--disable-gpu',
      '--disable-dev-shm-usage',
      '--disable-blink-features=AutomationControlled',
      '--remote-debugging-port=0',
      `--user-data-dir=${userDataDir}`,
      '--lang=zh-CN',
      'about:blank',
    ],
    { stdio: ['ignore', 'ignore', 'pipe'] },
  );
}

async function waitForDevToolsPort(userDataDir) {
  const portFile = path.join(userDataDir, 'DevToolsActivePort');
  for (let attempt = 0; attempt < 100; attempt += 1) {
    try {
      const [port, browserPath] = readFileSync(portFile, 'utf8').trim().split(/\r?\n/);
      return { port: Number(port), browserPath };
    } catch {
      await delay(100);
    }
  }
  throw new Error('Chrome did not expose DevToolsActivePort');
}

async function getPageWsUrl(port) {
  const response = await fetch(`http://127.0.0.1:${port}/json/list`);
  const targets = await response.json();
  const page = targets.find((target) => target.type === 'page');
  if (!page?.webSocketDebuggerUrl) {
    throw new Error('No Chrome page target found');
  }
  return page.webSocketDebuggerUrl;
}

function findTranslateControlExpression() {
  return `(() => {
    const clickableCandidates = Array.from(document.querySelectorAll('button, [role="button"], a'));
    const candidates = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'));
    const visibleText = (el) => {
      if (!el || /^(SCRIPT|STYLE|NOSCRIPT)$/i.test(el.tagName)) return '';
      const rect = el.getBoundingClientRect();
      if (rect.width <= 0 || rect.height <= 0) return '';
      return (el.innerText || el.getAttribute('aria-label') || '').trim();
    };
    let match = clickableCandidates.find((el) => isTranslateControl(el, visibleText(el)));
    if (!match) {
      match = candidates.find((el) => isTranslateControl(el, visibleText(el)));
    }
    if (match) {
      match = closestClickable(match);
    } else {
      const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
      while (walker.nextNode()) {
        const text = walker.currentNode.nodeValue?.trim() || '';
        if (!/显示翻译|翻译|Translate/i.test(text)) continue;
        const parent = walker.currentNode.parentElement;
        if (!isVisibleElement(parent)) continue;
        match = closestClickable(parent);
        if (match) break;
      }
    }
    if (!match) {
      return {
        found: false,
        candidates: candidates.map(visibleText).filter((text) => text && text.length <= 80).slice(0, 30),
      };
    }
    const text = visibleText(match);
    const rect = match.getBoundingClientRect();
    match.click();
    return {
      found: true,
      text,
      jsClick: true,
      x: rect.left + rect.width / 2,
      y: rect.top + rect.height / 2,
      tagName: match.tagName,
      role: match.getAttribute('role'),
    };

    function closestClickable(el) {
      for (let node = el; node && node !== document.body; node = node.parentElement) {
        if (node.tagName === 'BUTTON' || node.tagName === 'A' || node.getAttribute('role') === 'button') {
          return node;
        }
      }
      return el;
    }

    function isTranslateControl(el, text) {
      if (!text || text.length > 80) return false;
      if (!/显示翻译|翻译|Translate/i.test(text)) return false;
      return isVisibleElement(el);
    }

    function isVisibleElement(el) {
      if (!el || /^(SCRIPT|STYLE|NOSCRIPT)$/i.test(el.tagName)) return false;
      const rect = el.getBoundingClientRect();
      return rect.width > 0 && rect.height > 0 && rect.bottom > 0 && rect.right > 0;
    }
  })()`;
}

function pageSummaryExpression() {
  return `(() => {
    const body = document.body?.innerText || '';
    return {
      hasLoginText: /登录|Log in/.test(body),
      hasRetryText: /重试|Retry/.test(body),
      hasCannotTranslateText: /无法获取翻译|Could not translate/.test(body),
      excerpt: body.replace(/\\s+/g, ' ').slice(0, 600),
    };
  })()`;
}

function safeJson(text) {
  if (!text) return text;
  try {
    return JSON.parse(text);
  } catch {
    return redactText(text).slice(0, 1000);
  }
}

function redactHeaders(headers) {
  const redacted = {};
  for (const [key, value] of Object.entries(headers || {})) {
    if (/cookie|authorization|csrf|token/i.test(key)) {
      redacted[key] = '<redacted>';
    } else {
      redacted[key] = value;
    }
  }
  return redacted;
}

function redactText(text) {
  return String(text || '')
    .replace(/auth_token=[^;&\s]+/gi, 'auth_token=<redacted>')
    .replace(/ct0=[^;&\s]+/gi, 'ct0=<redacted>')
    .replace(/"authorization"\s*:\s*"[^"]+"/gi, '"authorization":"<redacted>"')
    .replace(/"x-csrf-token"\s*:\s*"[^"]+"/gi, '"x-csrf-token":"<redacted>"');
}

function redactPath(filePath) {
  return filePath.replace(/assets\/cookies\/[^/]+$/u, 'assets/cookies/<redacted>');
}

class CdpClient {
  static connect(url) {
    return new Promise((resolve, reject) => {
      const ws = new WebSocket(url);
      const client = new CdpClient(ws);
      ws.addEventListener('open', () => resolve(client), { once: true });
      ws.addEventListener('error', reject, { once: true });
    });
  }

  constructor(ws) {
    this.ws = ws;
    this.nextId = 1;
    this.pending = new Map();
    this.listeners = new Map();
    ws.addEventListener('message', (message) => this.handleMessage(message));
  }

  send(method, params = {}) {
    const id = this.nextId;
    this.nextId += 1;
    this.ws.send(JSON.stringify({ id, method, params }));
    return new Promise((resolve, reject) => {
      this.pending.set(id, { resolve, reject });
    });
  }

  async eval(expression) {
    const result = await this.send('Runtime.evaluate', {
      expression,
      awaitPromise: true,
      returnByValue: true,
    });
    return result.result?.value;
  }

  on(method, handler) {
    const handlers = this.listeners.get(method) || [];
    handlers.push(handler);
    this.listeners.set(method, handlers);
  }

  handleMessage(message) {
    const payload = JSON.parse(message.data);
    if (payload.id) {
      const pending = this.pending.get(payload.id);
      if (!pending) return;
      this.pending.delete(payload.id);
      if (payload.error) pending.reject(new Error(payload.error.message));
      else pending.resolve(payload.result);
      return;
    }
    for (const handler of this.listeners.get(payload.method) || []) {
      Promise.resolve(handler(payload.params)).catch(() => {});
    }
  }

  close() {
    this.ws.close();
  }
}
