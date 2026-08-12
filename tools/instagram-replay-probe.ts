#!/usr/bin/env bun
// IG live replay probe (no private API): reads today's video posts captured by the
// IG crawler (browser xdt timeline), uses yt-dlp (public extractor) to decide
// replay (duration >= 300s) and download the video into the replay archive.
import { spawnSync } from 'node:child_process'
import fs from 'node:fs'
import path from 'node:path'

const DB_PATH = process.env.IDOL_BBQ_DB_PATH || '/app/data.db'
const COOKIE_FILE = process.env.IG_COOKIE_FILE || '/app/assets/cookies/inscks0318.txt'
const YT_DLP = process.env.YT_DLP_PATH || '/app/tools/bin/yt-dlp'
const REPLAY_ROOT = process.env.IG_REPLAY_ROOT || '/app/ig-probe/replays'
const MIN_REPLAY_SECONDS = Number(process.env.IG_REPLAY_MIN_SECONDS || 300)
const HANDLES = (process.env.IG_REPLAY_HANDLES || 'nao_aikawa227,shiina_satsuki227,asaoka_mao__')
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)

function log(message: string) {
    process.stdout.write(`[ig-replay-probe] ${new Date().toISOString()} ${message}\n`)
}

function todayStartUtc(): number {
    const now = new Date()
    const jst = new Date(now.getTime() + 9 * 3600 * 1000)
    const start = new Date(Date.UTC(jst.getUTCFullYear(), jst.getUTCMonth(), jst.getUTCDate()))
    return Math.floor(start.getTime() / 1000) - 9 * 3600
}

function main() {
    if (!fs.existsSync(DB_PATH) || !fs.existsSync(COOKIE_FILE) || !fs.existsSync(YT_DLP)) {
        log(`missing prereq db=${fs.existsSync(DB_PATH)} cookie=${fs.existsSync(COOKIE_FILE)} ytdlp=${fs.existsSync(YT_DLP)}`)
        process.exit(1)
    }
    const from = todayStartUtc()
    const query = `select a_id,url from instagram_article where u_id in (${HANDLES.map(() => '?').join(',')}) and type='post' and created_at>=? order by created_at desc`
    const q = spawnSync('python3', ['-c', `import sqlite3,json,sys
c=sqlite3.connect(sys.argv[1])
c.row_factory=sqlite3.Row
rows=c.execute(sys.argv[2], sys.argv[3:]).fetchall()
print(json.dumps([dict(r) for r in rows]))`, DB_PATH, query, ...HANDLES, String(from)], { encoding: 'utf8', timeout: 30000 })
    if (q.status !== 0) {
        log(`db query failed: ${String(q.stderr || q.stdout || '').slice(0, 200)}`)
        process.exit(1)
    }
    let allRows: Array<{ a_id: string; url: string }> = []
    try {
        allRows = JSON.parse(q.stdout)
    } catch {
        log('db result parse failed')
        process.exit(1)
    }
    let downloaded = 0
    let skipped = 0
    for (const handle of HANDLES) {
        const _rows = allRows.filter((row: any) => row.url.includes(`/${handle}/`))
        for (const row of _rows as Array<{ a_id: string; url: string }>) {
            const outDir = path.join(REPLAY_ROOT, handle, row.a_id)
            const outFile = path.join(outDir, 'merged.mp4')
            if (fs.existsSync(outFile)) {
                skipped += 1
                continue
            }
            const probe = spawnSync(
                YT_DLP,
                ['--cookies', COOKIE_FILE, '--dump-single-json', '--no-download', '--skip-download', row.url],
                { encoding: 'utf8', timeout: 60000 },
            )
            if (probe.status !== 0) {
                log(`probe failed ${handle}/${row.a_id}: ${String(probe.stderr || '').slice(0, 120)}`)
                continue
            }
            let meta: any = null
            try {
                meta = JSON.parse(probe.stdout)
            } catch {
                log(`probe parse failed ${handle}/${row.a_id}`)
                continue
            }
            const duration = Number(meta?.duration || 0)
            if (!(duration >= MIN_REPLAY_SECONDS)) {
                continue
            }
            fs.mkdirSync(outDir, { recursive: true })
            fs.writeFileSync(path.join(outDir, 'broadcast.json'), JSON.stringify(meta, null, 2))
            log(`download replay ${handle}/${row.a_id} duration=${duration}s`)
            const dl = spawnSync(
                YT_DLP,
                ['--cookies', COOKIE_FILE, '-o', path.join(outDir, 'merged.%(ext)s'), row.url],
                { encoding: 'utf8', timeout: 3600000, maxBuffer: 8 * 1024 * 1024 },
            )
            if (dl.status !== 0) {
                log(`download failed ${handle}/${row.a_id}: ${String(dl.stderr || dl.stdout || '').slice(0, 200)}`)
                continue
            }
            const merged = path.join(outDir, 'merged.mp4')
            const webm = path.join(outDir, 'merged.webm')
            const src = fs.existsSync(merged) ? merged : webm
            if (src !== merged && fs.existsSync(src)) {
                fs.renameSync(src, merged)
            }
            const size = fs.existsSync(merged) ? fs.statSync(merged).size : 0
            log(`replay saved ${handle}/${row.a_id} size=${size}`)
            downloaded += 1
        }
    }
    log(`done downloaded=${downloaded} skipped=${skipped}`)
}

main()
