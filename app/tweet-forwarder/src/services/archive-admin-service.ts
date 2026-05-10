import { CACHE_DIR_ROOT } from '@/config'
import {
    buildCookieDocument,
    normalizeBiliupCookieDocument,
    resolveVideoUploadConfig,
    runBrowserCookieSync,
    type ResolvedBiliupVideoUploadConfig,
} from '@/middleware/forwarder/biliup'
import { ForwardTargetPlatformEnum, type AppConfig, type ForwardTarget } from '@/types'
import type { Logger } from '@idol-bbq-utils/log'
import { execFileSync, spawn } from 'child_process'
import crypto from 'crypto'
import fs from 'fs'
import path from 'path'

const ADMIN_ARCHIVE_CACHE_TTL_MS = 15_000
const DEFAULT_ARCHIVE_LIST_LIMIT = 80
const DEFAULT_FRAME_COUNT = 6
const WAVEFORM_WIDTH = 1280
const WAVEFORM_HEIGHT = 220
const RELATED_FILE_PREFIX_LIMIT = 48
const ARCHIVE_MANIFEST_FILE_NAME = 'archive-entry.json'
const ARCHIVE_MANIFEST_SUFFIX = '.archive.json'

const SUPPORTED_MEDIA_EXTENSIONS = new Set([
    '.ts',
    '.m2ts',
    '.mp4',
    '.mkv',
    '.mov',
    '.m4v',
    '.webm',
    '.flv',
    '.mp3',
    '.m4a',
    '.aac',
    '.wav',
    '.ogg',
])

type ArchiveKind = 'recording' | 'relay-session' | 'cache'

interface ArchiveSessionMetadata {
    pid?: string | null
    name?: string | null
    m3u8_name?: string | null
    reason?: string | null
    archived_at?: string | null
    session_started_at?: string | null
    source?: string | null
    page_url?: string | null
    segments?: Array<string>
}

interface ResolvedArchiveItem {
    id: string
    kind: ArchiveKind
    title: string
    fileName: string
    fileExtension: string
    mediaPath: string
    containerPath: string
    localPath: string
    sizeBytes: number
    modifiedAt: string
    createdAt: string
    category: string
    rootLabel: string
    session: ArchiveSessionMetadata | null
    remote?: {
        mode: 'http' | 'win-remote'
        target?: string
        mediaPath: string
        containerPath: string
        manifestPath?: string | null
        mediaUrl?: string | null
        manifestUrl?: string | null
        relatedFiles?: Array<ArchiveRelatedFile>
    } | null
}

interface ArchiveSummary {
    id: string
    kind: ArchiveKind
    title: string
    fileName: string
    fileExtension: string
    localPath: string
    sizeBytes: number
    modifiedAt: string
    createdAt: string
    category: string
    rootLabel: string
    pageUrl: string | null
    sourceUrl: string | null
}

interface ArchiveManifestRecord extends ArchiveSummary {
    version?: number
    visible?: boolean
    hiddenReason?: string | null
    mediaPath?: string
    mediaRelativePath?: string | null
    containerPath?: string
    containerRelativePath?: string | null
    manifestPath?: string | null
    manifestRelativePath?: string | null
    session?: ArchiveSessionMetadata | null
    relatedFiles?: Array<ArchiveRelatedFile & { relativePath?: string | null }>
}

interface ArchiveRelatedFile {
    name: string
    path: string
    sizeBytes: number
    modifiedAt: string
}

interface ArchiveUploadDefaults {
    cookieSourcePath: string
    helperPath: string
    pythonPath: string
    tid: number
    threads: number
    submitApi: string
    line: string
    copyright: number
    tags: Array<string>
}

interface ArchiveDetail extends ArchiveSummary {
    durationSeconds: number | null
    frameRate: number | null
    relatedFiles: Array<ArchiveRelatedFile>
    session: ArchiveSessionMetadata | null
    suggestedUpload: {
        title: string
        description: string
        sourceUrl: string
        tags: Array<string>
        cookieSourcePath: string
        tid: number
        threads: number
        submitApi: string
        line: string
        copyright: number
    }
}

interface ArchiveFramePreview {
    timeSeconds: number
    dataUrl: string
}

interface ArchiveFramePreviewBatch {
    frames: Array<ArchiveFramePreview>
    frameRate: number | null
    anchorTimeSeconds: number | null
    keyFrameTimes: Array<number>
}

interface ArchiveUploadRequest {
    title?: string
    description?: string
    sourceUrl?: string
    tags?: Array<string> | string
    tid?: number
    threads?: number
    submitApi?: string
    line?: string
    copyright?: number
    trimStartSeconds?: number
    trimEndSeconds?: number
    coverTimeSeconds?: number | null
    cookieSourcePath?: string
}

interface ArchiveUploadResult {
    ok: true
    title: string
    sourceUrl: string
    cookieSourcePath: string
    uploadedPath: string
    trimmedPath: string | null
    coverPath: string | null
    bvid: string | null
    aid: string | null
    videoUrl: string | null
    stdout: string
}

type BiliupCookieDocument = ReturnType<typeof buildCookieDocument>

interface ArchiveScanState {
    expiresAt: number
    items: Array<ResolvedArchiveItem>
    loaded: boolean
}

interface RemoteArchiveManifestRecord extends ArchiveManifestRecord {
    mediaPath: string
    containerPath: string
}

interface RemoteArchiveHttpConfig {
    mode: 'http'
    indexUrl: string
    filesBaseUrl: string
    stageRootDir: string
    requestHeaders: Array<string>
}

interface RemoteArchiveWinRemoteConfig {
    mode: 'win-remote'
    winRemotePath: string
    target: string
    archiveRootDir: string
    stageRootDir: string
}

type RemoteArchiveConfig = RemoteArchiveHttpConfig | RemoteArchiveWinRemoteConfig

const archiveScanState: ArchiveScanState = {
    expiresAt: 0,
    items: [],
    loaded: false,
}

function ensureDirectory(dirPath: string) {
    if (!fs.existsSync(dirPath)) {
        fs.mkdirSync(dirPath, { recursive: true })
    }
    return dirPath
}

function normalizeExtension(filePath: string) {
    return path.extname(filePath).toLowerCase()
}

function isSupportedMediaFile(filePath: string) {
    return SUPPORTED_MEDIA_EXTENSIONS.has(normalizeExtension(filePath))
}

function toIsoString(stats: fs.Stats) {
    return stats.mtime.toISOString()
}

function uniqueStrings(values: Array<string | undefined | null>) {
    return Array.from(new Set(values.map((value) => String(value || '').trim()).filter(Boolean)))
}

function hashText(value: string) {
    return crypto.createHash('sha1').update(value).digest('hex')
}

function createArchiveId(kind: ArchiveKind, containerPath: string, mediaPath: string) {
    return hashText(`${kind}\n${path.resolve(containerPath)}\n${path.resolve(mediaPath)}`)
}

function sanitizeSegment(value: string, fallback: string) {
    const normalized = String(value || '')
        .replace(/[<>:"/\\|?*\u0000-\u001F]+/g, ' ')
        .replace(/\s+/g, ' ')
        .trim()
    return normalized || fallback
}

function safeJsonParse<T>(value: string): T | null {
    try {
        return JSON.parse(value) as T
    } catch {
        return null
    }
}

function splitEnvPaths(value?: string) {
    return String(value || '')
        .split(/\r?\n|,/)
        .map((entry) => entry.trim())
        .filter(Boolean)
}

function isTruthyFlag(value: unknown) {
    const normalized = String(value || '').trim().toLowerCase()
    return normalized === '1' || normalized === 'true' || normalized === 'yes' || normalized === 'on'
}

function getWorkspaceAncestors() {
    const roots: string[] = []
    let current = process.cwd()
    while (current && !roots.includes(current)) {
        roots.push(current)
        const parent = path.dirname(current)
        if (parent === current) {
            break
        }
        current = parent
    }
    return roots
}

function discoverSiblingStreamServDirs() {
    const candidates: string[] = []
    for (const root of getWorkspaceAncestors()) {
        const direct = path.join(root, 'StreamServ')
        if (fs.existsSync(direct) && fs.statSync(direct).isDirectory()) {
            candidates.push(direct)
        }
        const underApp = path.join(root, '..', 'StreamServ')
        if (fs.existsSync(underApp) && fs.statSync(underApp).isDirectory()) {
            candidates.push(path.resolve(underApp))
        }
    }
    return uniqueStrings(candidates)
}

function resolveWinRemotePath() {
    const candidates = [
        process.env.STREAMSERV_WIN_REMOTE_PATH,
        process.env.WINDOWS_REMOTE_EXECUTOR_PATH,
        ...getWorkspaceAncestors().flatMap((root) => [
            path.join(root, 'windows-remote-executor', 'bin', 'win-remote'),
            path.resolve(root, '..', 'windows-remote-executor', 'bin', 'win-remote'),
        ]),
    ]
    return uniqueStrings(candidates).find((candidate) => fs.existsSync(candidate) && fs.statSync(candidate).isFile()) || null
}

function normalizeUrlPrefix(value: string) {
    const trimmed = String(value || '').trim()
    if (!trimmed) {
        return ''
    }
    return trimmed.endsWith('/') ? trimmed : `${trimmed}/`
}

function parseRequestHeaders(rawValue?: string) {
    const values = uniqueStrings([
        rawValue,
        process.env.STREAMSERV_REMOTE_ARCHIVE_HEADER,
        process.env.WAF_BYPASS_HEADER,
        'x-bypass-waf: N2NJ_SUPER_SECRET_PASS_2026_7684',
    ])

    return values
        .flatMap((value) => value.split(/\r?\n/).map((entry) => entry.trim()).filter(Boolean))
        .filter((entry) => entry.includes(':'))
}

function resolveRemoteArchiveHttpConfig(): RemoteArchiveHttpConfig | null {
    const baseUrl = normalizeUrlPrefix(
        process.env.STREAMSERV_REMOTE_ARCHIVE_BASE_URL
        || process.env.STREAMSERV_REMOTE_ARCHIVE_URL
        || 'http://100.119.106.8:22781/archive-admin/',
    )
    if (!baseUrl) {
        return null
    }

    const indexUrl = String(process.env.STREAMSERV_REMOTE_ARCHIVE_INDEX_URL || `${baseUrl}index.json`).trim()
    if (!indexUrl) {
        return null
    }

    return {
        mode: 'http',
        indexUrl,
        filesBaseUrl: baseUrl,
        stageRootDir: ensureDirectory(path.join(CACHE_DIR_ROOT, 'archive-admin', 'remote-media')),
        requestHeaders: parseRequestHeaders(process.env.STREAMSERV_REMOTE_ARCHIVE_HEADER),
    }
}

function resolveRemoteArchiveConfig(): RemoteArchiveConfig | null {
    const disabled = String(process.env.STREAMSERV_ENABLE_REMOTE_ARCHIVES || '').trim().toLowerCase()
    if (disabled === '0' || disabled === 'false' || disabled === 'no' || disabled === 'off') {
        return null
    }

    const preferredMode = String(process.env.STREAMSERV_REMOTE_ARCHIVE_MODE || '').trim().toLowerCase()
    const httpConfig = resolveRemoteArchiveHttpConfig()
    if (httpConfig && preferredMode !== 'win-remote') {
        return httpConfig
    }

    const winRemotePath = resolveWinRemotePath()
    if (!winRemotePath) {
        return httpConfig
    }

    return {
        mode: 'win-remote',
        winRemotePath,
        target: String(process.env.STREAMSERV_REMOTE_TARGET || 'X570').trim() || 'X570',
        archiveRootDir: String(process.env.STREAMSERV_REMOTE_ARCHIVE_ROOT_DIR || 'D:/RZ2/StreamServArchive').trim(),
        stageRootDir: ensureDirectory(path.join(CACHE_DIR_ROOT, 'archive-admin', 'remote-media')),
    }
}

function runRemoteCurl(remote: RemoteArchiveHttpConfig, extraArgs: Array<string>, options?: { encoding?: BufferEncoding }) {
    const args = ['-fsSL', '--retry', '2', '--connect-timeout', '5', '--max-time', '300']
    for (const header of remote.requestHeaders) {
        args.push('-H', header)
    }
    args.push(...extraArgs)
    return execFileSync('curl', args, {
        encoding: options?.encoding,
        stdio: ['ignore', 'pipe', 'pipe'],
    })
}

function runWinRemoteExec(remote: RemoteArchiveWinRemoteConfig, script: string) {
    return execFileSync(
        remote.winRemotePath,
        ['exec', remote.target, '--stdin'],
        {
            input: script,
            encoding: 'utf8',
            stdio: ['pipe', 'pipe', 'pipe'],
        },
    )
}

function tryParseRemoteJson<T>(remote: RemoteArchiveConfig, source: string): T | null {
    try {
        const output = remote.mode === 'http'
            ? String(runRemoteCurl(remote, [source], { encoding: 'utf8' })).trim()
            : runWinRemoteExec(remote, source).trim()
        if (!output) {
            return null
        }
        return safeJsonParse<T>(output)
    } catch {
        return null
    }
}

function normalizeJsonArray<T>(value: T | Array<T> | null | undefined) {
    if (!value) {
        return [] as Array<T>
    }
    return Array.isArray(value) ? value : [value]
}

function existingDirectories(candidates: Array<string>) {
    return uniqueStrings(candidates)
        .map((entry) => path.resolve(entry))
        .filter((entry) => fs.existsSync(entry) && fs.statSync(entry).isDirectory())
}

function resolveArchiveRootCandidates() {
    const streamServDirs = discoverSiblingStreamServDirs()
    const candidates = [
        ...splitEnvPaths(process.env.STREAMSERV_ARCHIVE_ROOTS),
        ...splitEnvPaths(process.env.STREAMSERV_ARCHIVE_ROOT_DIR),
        path.join(process.cwd(), 'StreamServArchive'),
        path.join(process.cwd(), 'archive'),
        ...streamServDirs.flatMap((dir) => [
            path.join(dir, 'D:/RZ2/StreamServArchive'),
            path.join(dir, 'archive'),
            dir,
        ]),
    ]
    return existingDirectories(candidates)
}

function resolveCacheRootCandidates() {
    const streamServDirs = discoverSiblingStreamServDirs()
    const candidates = [
        ...splitEnvPaths(process.env.STREAMSERV_CACHE_ROOTS),
        ...splitEnvPaths(process.env.STREAMSERV_CACHE_DIR),
        path.join(CACHE_DIR_ROOT, 'media'),
        ...streamServDirs.flatMap((dir) => [
            path.join(dir, 'D:/StreamServ/cache'),
            path.join(dir, 'cache'),
        ]),
    ]
    return existingDirectories(candidates)
}

function parseSessionMetadata(sessionDir: string) {
    const sessionPath = path.join(sessionDir, 'session.json')
    if (!fs.existsSync(sessionPath)) {
        return null
    }
    return safeJsonParse<ArchiveSessionMetadata>(fs.readFileSync(sessionPath, 'utf8'))
}

function choosePrimaryRelayMedia(sessionDir: string) {
    const entries = fs
        .readdirSync(sessionDir)
        .map((name) => path.join(sessionDir, name))
        .filter((filePath) => fs.existsSync(filePath) && fs.statSync(filePath).isFile() && isSupportedMediaFile(filePath))
        .sort((left, right) => {
            const leftBase = path.basename(left).toLowerCase()
            const rightBase = path.basename(right).toLowerCase()
            const leftMerged = leftBase.includes('_merged.')
            const rightMerged = rightBase.includes('_merged.')
            if (leftMerged !== rightMerged) {
                return leftMerged ? -1 : 1
            }
            return fs.statSync(right).size - fs.statSync(left).size
        })
    return entries[0] || null
}

function createArchiveItem(
    kind: ArchiveKind,
    mediaPath: string,
    containerPath: string,
    category: string,
    rootLabel: string,
    session: ArchiveSessionMetadata | null = null,
): ResolvedArchiveItem | null {
    if (!fs.existsSync(mediaPath) || !fs.statSync(mediaPath).isFile() || !isSupportedMediaFile(mediaPath)) {
        return null
    }
    const stats = fs.statSync(mediaPath)
    const fileName = path.basename(mediaPath)
    const titleBase = sanitizeSegment(path.basename(mediaPath, path.extname(mediaPath)), fileName)
    return {
        id: createArchiveId(kind, containerPath, mediaPath),
        kind,
        title: sanitizeSegment(session?.name || titleBase, titleBase),
        fileName,
        fileExtension: normalizeExtension(mediaPath),
        mediaPath,
        containerPath,
        localPath: mediaPath,
        sizeBytes: stats.size,
        modifiedAt: stats.mtime.toISOString(),
        createdAt: stats.birthtime.toISOString(),
        category,
        rootLabel,
        session,
        remote: null,
    }
}

function walkRecordingFiles(dirPath: string, depth: number, maxDepth: number, collected: Array<string>) {
    if (depth > maxDepth || !fs.existsSync(dirPath)) {
        return
    }
    for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
        const fullPath = path.join(dirPath, entry.name)
        if (entry.isDirectory()) {
            if (entry.name === 'relay_sessions') {
                continue
            }
            walkRecordingFiles(fullPath, depth + 1, maxDepth, collected)
            continue
        }
        if (entry.isFile() && isSupportedMediaFile(fullPath)) {
            collected.push(fullPath)
        }
    }
}

function shouldIncludeCacheArchives() {
    const raw = String(process.env.STREAMSERV_INCLUDE_CACHE_ARCHIVES || '').trim().toLowerCase()
    return raw === '1' || raw === 'true' || raw === 'yes'
}

function isArchiveManifestFile(filePath: string) {
    const baseName = path.basename(filePath).toLowerCase()
    return baseName === ARCHIVE_MANIFEST_FILE_NAME || baseName.endsWith(ARCHIVE_MANIFEST_SUFFIX)
}

function walkManifestFiles(dirPath: string, depth: number, maxDepth: number, collected: Array<string>) {
    if (depth > maxDepth || !fs.existsSync(dirPath)) {
        return
    }
    for (const entry of fs.readdirSync(dirPath, { withFileTypes: true })) {
        const fullPath = path.join(dirPath, entry.name)
        if (entry.isDirectory()) {
            walkManifestFiles(fullPath, depth + 1, maxDepth, collected)
            continue
        }
        if (entry.isFile() && isArchiveManifestFile(fullPath)) {
            collected.push(fullPath)
        }
    }
}

function createArchiveItemFromManifest(filePath: string) {
    const payload = safeJsonParse<ArchiveManifestRecord>(fs.readFileSync(filePath, 'utf8'))
    if (!payload || payload.visible === false) {
        return null
    }

    const mediaPath = path.resolve(String(payload.mediaPath || payload.localPath || ''))
    const containerPath = path.resolve(String(payload.containerPath || path.dirname(mediaPath)))
    if (!mediaPath || !fs.existsSync(mediaPath) || !fs.statSync(mediaPath).isFile() || !isSupportedMediaFile(mediaPath)) {
        return null
    }

    const stats = fs.statSync(mediaPath)
    const fileName = String(payload.fileName || path.basename(mediaPath))
    const fileExtension = String(payload.fileExtension || normalizeExtension(mediaPath))
    const title = sanitizeSegment(String(payload.title || path.basename(mediaPath, path.extname(mediaPath))), fileName)

    return {
        id: String(payload.id || createArchiveId((payload.kind || 'recording') as ArchiveKind, containerPath, mediaPath)),
        kind: (payload.kind || 'recording') as ArchiveKind,
        title,
        fileName,
        fileExtension,
        mediaPath,
        containerPath,
        localPath: mediaPath,
        sizeBytes: stats.size,
        modifiedAt: String(payload.modifiedAt || stats.mtime.toISOString()),
        createdAt: String(payload.createdAt || stats.birthtime.toISOString()),
        category: String(payload.category || path.basename(path.dirname(mediaPath))),
        rootLabel: String(payload.rootLabel || path.basename(containerPath)),
        session: payload.session || null,
        remote: null,
    } satisfies ResolvedArchiveItem
}

function createRemoteArchiveItemFromManifest(
    payload: RemoteArchiveManifestRecord,
    remote: RemoteArchiveConfig,
    manifestPath: string,
) {
    const mediaPath = String(payload.mediaPath || payload.localPath || '').trim()
    const containerPath = String(payload.containerPath || '').trim()
    if (!mediaPath || !containerPath || !isSupportedMediaFile(mediaPath)) {
        return null
    }

    const mediaRelativePath = String(payload.mediaRelativePath || '').trim()
    const manifestRelativePath = String(payload.manifestRelativePath || '').trim()
    const mediaUrl = remote.mode === 'http' && mediaRelativePath
        ? new URL(mediaRelativePath.replace(/^\/+/, ''), remote.filesBaseUrl).toString()
        : null
    const manifestUrl = remote.mode === 'http' && manifestRelativePath
        ? new URL(manifestRelativePath.replace(/^\/+/, ''), remote.filesBaseUrl).toString()
        : null
    if (remote.mode === 'http' && !mediaUrl) {
        return null
    }

    return {
        id: String(payload.id || hashText(`${payload.kind || 'recording'}\n${containerPath}\n${mediaPath}`)),
        kind: (payload.kind || 'recording') as ArchiveKind,
        title: sanitizeSegment(String(payload.title || path.basename(mediaPath, path.extname(mediaPath))), path.basename(mediaPath)),
        fileName: String(payload.fileName || path.basename(mediaPath)),
        fileExtension: String(payload.fileExtension || normalizeExtension(mediaPath)),
        mediaPath,
        containerPath,
        localPath: String(payload.localPath || mediaPath),
        sizeBytes: Number(payload.sizeBytes || 0),
        modifiedAt: String(payload.modifiedAt || new Date().toISOString()),
        createdAt: String(payload.createdAt || payload.modifiedAt || new Date().toISOString()),
        category: String(payload.category || path.basename(path.dirname(mediaPath))),
        rootLabel: String(payload.rootLabel || path.basename(containerPath)),
        session: payload.session || null,
        remote: {
            mode: remote.mode,
            target: remote.mode === 'win-remote' ? remote.target : undefined,
            mediaPath,
            containerPath,
            manifestPath,
            mediaUrl,
            manifestUrl,
            relatedFiles: normalizeJsonArray(payload.relatedFiles).map((entry) => ({
                name: String(entry?.name || path.basename(String(entry?.path || '')) || 'unknown'),
                path: String((entry as any)?.path || ''),
                sizeBytes: Number((entry as any)?.sizeBytes || 0),
                modifiedAt: String((entry as any)?.modifiedAt || ''),
            })),
        },
    } satisfies ResolvedArchiveItem
}

function scanRemoteArchiveManifestRoot() {
    const remote = resolveRemoteArchiveConfig()
    if (!remote) {
        return [] as Array<ResolvedArchiveItem>
    }

    if (remote.mode === 'http') {
        const raw = tryParseRemoteJson<
            { items?: Array<RemoteArchiveManifestRecord> } | Array<RemoteArchiveManifestRecord>
        >(remote, remote.indexUrl)
        const records = Array.isArray(raw) ? raw : normalizeJsonArray(raw?.items)
        return records
            .map((payload) => createRemoteArchiveItemFromManifest(payload, remote, String(payload.manifestPath || '').trim()))
            .filter((item): item is ResolvedArchiveItem => Boolean(item))
    }

    const archiveRoot = remote.archiveRootDir.replace(/\//g, '\\')
    const script = [
        `$root = '${archiveRoot.replace(/'/g, "''")}'`,
        "if (-not (Test-Path $root)) { '[]'; exit 0 }",
        '$records = @()',
        "Get-ChildItem $root -Recurse -File -ErrorAction SilentlyContinue | Where-Object { $_.Name -ieq 'archive-entry.json' -or $_.Name -like '*.archive.json' } | ForEach-Object {",
        '  try {',
        "    $payload = Get-Content -Path $_.FullName -Raw -ErrorAction Stop | ConvertFrom-Json -ErrorAction Stop",
        '    if ($null -ne $payload -and $payload.visible -ne $false) {',
        "      $records += [pscustomobject]@{ manifestPath = $_.FullName; payload = $payload }",
        '    }',
        '  } catch { }',
        '}',
        '$records | ConvertTo-Json -Compress -Depth 12',
    ].join('\n')

    const raw = tryParseRemoteJson<
        Array<{ manifestPath?: string; payload?: RemoteArchiveManifestRecord }> | { manifestPath?: string; payload?: RemoteArchiveManifestRecord }
    >(remote, script)
    return normalizeJsonArray(raw)
        .map((entry) =>
            entry?.payload
                ? createRemoteArchiveItemFromManifest(entry.payload, remote, String(entry.manifestPath || '').trim())
                : null,
        )
        .filter((item): item is ResolvedArchiveItem => Boolean(item))
}

function scanArchiveManifestRoot(rootDir: string) {
    const manifestFiles: Array<string> = []
    walkManifestFiles(rootDir, 0, 4, manifestFiles)
    const items = manifestFiles
        .map((filePath) => createArchiveItemFromManifest(filePath))
        .filter((item): item is ResolvedArchiveItem => Boolean(item))

    return {
        manifestCount: manifestFiles.length,
        items,
    }
}

function scanRelaySessionsDir(relayDir: string) {
    const items: Array<ResolvedArchiveItem> = []
    for (const entry of fs.readdirSync(relayDir, { withFileTypes: true })) {
        if (!entry.isDirectory()) {
            continue
        }
        const sessionDir = path.join(relayDir, entry.name)
        const mediaPath = choosePrimaryRelayMedia(sessionDir)
        if (!mediaPath) {
            continue
        }
        const item = createArchiveItem(
            'relay-session',
            mediaPath,
            sessionDir,
            'relay_sessions',
            path.basename(relayDir),
            parseSessionMetadata(sessionDir),
        )
        if (item) {
            items.push(item)
        }
    }
    return items
}

function scanRecordingRoot(rootDir: string, rootLabel: string) {
    const files: Array<string> = []
    walkRecordingFiles(rootDir, 0, 3, files)
    return files
        .map((mediaPath) =>
            createArchiveItem(
                'recording',
                mediaPath,
                path.dirname(mediaPath),
                path.relative(rootDir, path.dirname(mediaPath)) || '.',
                rootLabel,
            ),
        )
        .filter((item): item is ResolvedArchiveItem => Boolean(item))
}

function scanArchiveRoot(rootDir: string) {
    const items: Array<ResolvedArchiveItem> = []
    const relayDir = path.basename(rootDir) === 'relay_sessions' ? rootDir : path.join(rootDir, 'relay_sessions')
    if (fs.existsSync(relayDir) && fs.statSync(relayDir).isDirectory()) {
        items.push(...scanRelaySessionsDir(relayDir))
    }

    const recordingDir = path.basename(rootDir) === 'recordings' ? rootDir : path.join(rootDir, 'recordings')
    if (fs.existsSync(recordingDir) && fs.statSync(recordingDir).isDirectory()) {
        items.push(...scanRecordingRoot(recordingDir, path.basename(rootDir)))
        return items
    }

    if (path.basename(rootDir) !== 'relay_sessions') {
        items.push(...scanRecordingRoot(rootDir, path.basename(rootDir)))
    }
    return items
}

function scanCacheRoot(rootDir: string) {
    return fs
        .readdirSync(rootDir, { withFileTypes: true })
        .filter((entry) => entry.isFile())
        .map((entry) =>
            createArchiveItem('cache', path.join(rootDir, entry.name), rootDir, 'cache', path.basename(rootDir)),
        )
        .filter((item): item is ResolvedArchiveItem => Boolean(item))
}

function getArchiveAccessPath(item: ResolvedArchiveItem) {
    if (!item.remote) {
        return item.mediaPath
    }

    const remote = resolveRemoteArchiveConfig()
    if (!remote) {
        throw new Error('Remote archive access is unavailable')
    }

    const archiveDir = ensureDirectory(path.join(remote.stageRootDir, item.id))
    const localPath = path.join(archiveDir, item.fileName)
    if (fs.existsSync(localPath) && fs.statSync(localPath).isFile() && fs.statSync(localPath).size === item.sizeBytes) {
        return localPath
    }

    if (remote.mode === 'http') {
        const mediaUrl = String(item.remote.mediaUrl || '').trim()
        if (!mediaUrl) {
            throw new Error(`Remote archive media URL is unavailable: ${item.fileName}`)
        }
        runRemoteCurl(remote, ['-o', localPath, mediaUrl])
    } else {
        execFileSync(
            remote.winRemotePath,
            ['get', String(item.remote.target || remote.target), item.remote.mediaPath.replace(/\\/g, '/'), localPath],
            {
                stdio: ['ignore', 'pipe', 'pipe'],
            },
        )
    }

    if (!fs.existsSync(localPath) || fs.statSync(localPath).size <= 0) {
        throw new Error(`Failed to stage remote archive media: ${item.remote.mediaPath}`)
    }
    return localPath
}

function resolveArchiveRelatedFiles(item: ResolvedArchiveItem) {
    if (!item.remote) {
        return resolveRelatedFiles(item)
    }

    if (item.remote.relatedFiles && item.remote.relatedFiles.length > 0) {
        return [...item.remote.relatedFiles].sort((left, right) => right.sizeBytes - left.sizeBytes)
    }

    const remote = resolveRemoteArchiveConfig()
    if (!remote || remote.mode !== 'win-remote') {
        return [
            {
                name: item.fileName,
                path: item.remote.mediaUrl || item.remote.mediaPath,
                sizeBytes: item.sizeBytes,
                modifiedAt: item.modifiedAt,
            },
        ]
    }

    const containerPath = item.remote.containerPath.replace(/\\/g, '\\')
    const script = [
        `$dir = '${containerPath.replace(/'/g, "''")}'`,
        "if (-not (Test-Path $dir)) { '[]'; exit 0 }",
        "Get-ChildItem $dir -File -ErrorAction SilentlyContinue | Select-Object @{Name='name';Expression={$_.Name}}, @{Name='path';Expression={$_.FullName}}, @{Name='sizeBytes';Expression={[int64]$_.Length}}, @{Name='modifiedAt';Expression={$_.LastWriteTime.ToString('o')}} | ConvertTo-Json -Compress -Depth 4",
    ].join('\n')
    const raw = tryParseRemoteJson<Array<ArchiveRelatedFile> | ArchiveRelatedFile>(remote, script)
    return normalizeJsonArray(raw).sort((left, right) => right.sizeBytes - left.sizeBytes)
}

function summarizeArchive(item: ResolvedArchiveItem): ArchiveSummary {
    return {
        id: item.id,
        kind: item.kind,
        title: item.title,
        fileName: item.fileName,
        fileExtension: item.fileExtension,
        localPath: item.localPath,
        sizeBytes: item.sizeBytes,
        modifiedAt: item.modifiedAt,
        createdAt: item.createdAt,
        category: item.category,
        rootLabel: item.rootLabel,
        pageUrl: item.session?.page_url || null,
        sourceUrl: item.session?.source || null,
    }
}

function getResolvedArchives(force = false) {
    const now = Date.now()
    if (!force && archiveScanState.loaded && archiveScanState.expiresAt > now) {
        return archiveScanState.items
    }

    const archiveRoots = resolveArchiveRootCandidates()
    const manifestScans = archiveRoots.map((rootDir) => scanArchiveManifestRoot(rootDir))
    const manifestCount = manifestScans.reduce((sum, scan) => sum + scan.manifestCount, 0)
    const archiveItems = manifestCount > 0
        ? manifestScans.flatMap((scan) => scan.items)
        : archiveRoots.flatMap((rootDir) => scanArchiveRoot(rootDir))
    const remoteArchiveItems = scanRemoteArchiveManifestRoot()

    const items = [
        ...archiveItems,
        ...remoteArchiveItems,
        ...(shouldIncludeCacheArchives() ? resolveCacheRootCandidates().flatMap((rootDir) => scanCacheRoot(rootDir)) : []),
    ]
        .filter((item, index, arr) => arr.findIndex((entry) => entry.id === item.id) === index)
        .sort((left, right) => new Date(right.modifiedAt).getTime() - new Date(left.modifiedAt).getTime())

    archiveScanState.items = items
    archiveScanState.expiresAt = now + ADMIN_ARCHIVE_CACHE_TTL_MS
    archiveScanState.loaded = true
    return items
}

function findArchiveOrThrow(archiveId: string) {
    const item = getResolvedArchives().find((entry) => entry.id === archiveId)
    if (!item) {
        throw new Error(`Archive not found: ${archiveId}`)
    }
    return item
}

function resolveRelatedFiles(item: ResolvedArchiveItem) {
    if (!fs.existsSync(item.containerPath)) {
        return []
    }

    const prefix = path.basename(item.mediaPath, path.extname(item.mediaPath))
    const related = fs
        .readdirSync(item.containerPath)
        .slice(0, RELATED_FILE_PREFIX_LIMIT)
        .map((name) => path.join(item.containerPath, name))
        .filter((filePath) => fs.existsSync(filePath) && fs.statSync(filePath).isFile())
        .filter((filePath) => {
            if (item.kind === 'relay-session') {
                return true
            }
            return path.basename(filePath).startsWith(prefix)
        })
        .map((filePath) => {
            const stats = fs.statSync(filePath)
            return {
                name: path.basename(filePath),
                path: filePath,
                sizeBytes: stats.size,
                modifiedAt: stats.mtime.toISOString(),
            }
        })
        .sort((left, right) => right.sizeBytes - left.sizeBytes)

    return related
}

function resolveFfprobePath() {
    return process.env.FFPROBE_PATH || 'ffprobe'
}

function resolveFfmpegPath(uploadConfig?: ResolvedBiliupVideoUploadConfig | null) {
    return uploadConfig?.collision_placeholder_part?.ffmpeg_path || process.env.FFMPEG_PATH || 'ffmpeg'
}

function probeMediaDetails(filePath: string) {
    try {
        const output = execFileSync(
            resolveFfprobePath(),
            ['-v', 'error', '-show_streams', '-show_format', '-print_format', 'json', filePath],
            { encoding: 'utf8' },
        )
        const payload = safeJsonParse<{
            format?: { duration?: string }
            streams?: Array<{ codec_type?: string; avg_frame_rate?: string; r_frame_rate?: string }>
        }>(output)
        const duration = Number(payload?.format?.duration || 0)
        const videoStream = (payload?.streams || []).find((stream) => String(stream.codec_type || '').toLowerCase() === 'video')
        const frameRate = parseFrameRate(videoStream?.avg_frame_rate) || parseFrameRate(videoStream?.r_frame_rate)
        return {
            durationSeconds: Number.isFinite(duration) && duration > 0 ? duration : null,
            frameRate,
        }
    } catch {
        return {
            durationSeconds: null,
            frameRate: null,
        }
    }
}

function parseFrameRate(value?: string) {
    const normalized = String(value || '').trim()
    if (!normalized || normalized === '0/0') {
        return null
    }

    if (!normalized.includes('/')) {
        const parsed = Number(normalized)
        return Number.isFinite(parsed) && parsed > 0 ? parsed : null
    }

    const [rawNumerator, rawDenominator] = normalized.split('/', 2)
    const numerator = Number(rawNumerator)
    const denominator = Number(rawDenominator)
    if (!Number.isFinite(numerator) || !Number.isFinite(denominator) || numerator <= 0 || denominator <= 0) {
        return null
    }
    const rate = numerator / denominator
    return Number.isFinite(rate) && rate > 0 ? rate : null
}

function clampNumber(value: unknown, min: number, max: number) {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) {
        return min
    }
    return Math.min(max, Math.max(min, numeric))
}

function resolveAdminUploadTarget(config: AppConfig) {
    const target = (config.forward_targets || []).find(
        (entry) => String(entry.platform) === ForwardTargetPlatformEnum.Bilibili,
    ) as ForwardTarget<ForwardTargetPlatformEnum.Bilibili> | undefined

    const platformConfig = (target?.cfg_platform || {}) as ForwardTarget<ForwardTargetPlatformEnum.Bilibili>['cfg_platform']
    const uploadConfig = resolveVideoUploadConfig({
        enabled: true,
        ...(platformConfig.video_upload || {}),
    })

    if (!uploadConfig) {
        throw new Error('Bilibili upload configuration is unavailable')
    }

    return {
        target,
        platformConfig,
        uploadConfig,
        sessdata: String((platformConfig as any).sessdata || '').trim(),
        bili_jct: String((platformConfig as any).bili_jct || '').trim(),
    }
}

function discoverDefaultCookieSourcePath(config: AppConfig) {
    const uploadTarget = resolveAdminUploadTarget(config)
    const configuredCookie = String(uploadTarget.uploadConfig.cookie_file || '').trim()
    const candidates = [
        process.env.ARCHIVE_UPLOAD_COOKIE_SOURCE,
        process.env.BILIUP_COOKIE_SOURCE,
        process.env.BILIUP_COOKIE_NETSCAPE_PATH,
        process.env.COOKIE_NETSCAPE_PATH,
        ...getWorkspaceAncestors().flatMap((root) => [
            path.join(root, 'a2sibck.txt'),
            path.join(root, 'cookies.a2si.txt'),
        ]),
        configuredCookie,
    ]
    for (const candidate of candidates) {
        if (!candidate) {
            continue
        }
        const resolved = path.resolve(candidate)
        if (fs.existsSync(resolved) && fs.statSync(resolved).isFile()) {
            return resolved
        }
    }
    return configuredCookie || ''
}

function buildArchiveUploadDefaults(config: AppConfig): ArchiveUploadDefaults {
    const uploadTarget = resolveAdminUploadTarget(config)
    return {
        cookieSourcePath: discoverDefaultCookieSourcePath(config),
        helperPath: uploadTarget.uploadConfig.helper_path,
        pythonPath: uploadTarget.uploadConfig.python_path,
        tid: uploadTarget.uploadConfig.tid,
        threads: uploadTarget.uploadConfig.threads,
        submitApi: uploadTarget.uploadConfig.submit_api,
        line: uploadTarget.uploadConfig.line,
        copyright: uploadTarget.uploadConfig.copyright,
        tags: [...uploadTarget.uploadConfig.tags],
    }
}

function buildSuggestedUpload(item: ResolvedArchiveItem, config: AppConfig) {
    const defaults = buildArchiveUploadDefaults(config)
    const titleBase = sanitizeSegment(path.basename(item.fileName, item.fileExtension), item.fileName)
    const descriptionLines = [
        item.session?.name ? `来源频道: ${item.session.name}` : '',
        item.session?.archived_at ? `存档时间: ${item.session.archived_at}` : `本地文件: ${item.localPath}`,
        item.session?.page_url ? `原页面: ${item.session.page_url}` : '',
        item.session?.source ? `原始流: ${item.session.source}` : '',
    ].filter(Boolean)

    return {
        title: titleBase,
        description: descriptionLines.join('\n'),
        sourceUrl: item.session?.page_url || item.session?.source || '',
        tags: defaults.tags,
        cookieSourcePath: defaults.cookieSourcePath,
        tid: defaults.tid,
        threads: defaults.threads,
        submitApi: defaults.submitApi,
        line: defaults.line,
        copyright: defaults.copyright,
    }
}

function parseNetscapeCookieFile(cookiePath: string) {
    const cookies: Array<Record<string, unknown>> = []
    for (const rawLine of fs.readFileSync(cookiePath, 'utf8').split(/\r?\n/)) {
        let line = rawLine.trim()
        let httpOnly = 0
        if (line.startsWith('#HttpOnly_')) {
            line = line.slice('#HttpOnly_'.length)
            httpOnly = 1
        }
        if (!line || line.startsWith('#')) {
            continue
        }
        const parts = line.split('\t')
        if (parts.length !== 7) {
            continue
        }
        const [domain, _subdomains, cookiePathValue, secure, expires, name, value] = parts
        const secureFlag = String(secure || '').toUpperCase() === 'TRUE'
        cookies.push({
            domain,
            path: cookiePathValue,
            name,
            value,
            expires: Number(expires || 0) || 0,
            secure: secureFlag ? 1 : 0,
            http_only: httpOnly,
        })
    }
    return cookies
}

function buildCookieDocumentFromNetscape(cookiePath: string) {
    const cookies = parseNetscapeCookieFile(cookiePath).filter((cookie) => {
        const domain = String(cookie.domain || '').replace(/^\./, '').toLowerCase()
        return domain.endsWith('bilibili.com') || domain.endsWith('hdslb.com') || domain.endsWith('bilivideo.com')
    })

    const names = new Set(cookies.map((cookie) => String(cookie.name || '').trim()))
    const missing = ['SESSDATA', 'bili_jct'].filter((name) => !names.has(name))
    if (missing.length > 0) {
        throw new Error(`Missing required Bilibili cookies in Netscape source: ${missing.join(', ')}`)
    }

    return normalizeBiliupCookieDocument({
        cookie_info: {
            cookies,
        },
        sso: [],
        token_info: {
            access_token: '',
            expires_in: 0,
            mid: 0,
            refresh_token: '',
        },
        platform: null,
    })
}

function isProbablyNetscapeCookie(content: string) {
    return content.includes('# Netscape HTTP Cookie File') || /\t(?:TRUE|FALSE)\t/.test(content)
}

async function loadCookieDocument(
    config: AppConfig,
    explicitSourcePath: string,
    log?: Logger,
): Promise<{ document: BiliupCookieDocument; sourcePath: string }> {
    const uploadTarget = resolveAdminUploadTarget(config)
    const sourcePath = String(explicitSourcePath || discoverDefaultCookieSourcePath(config) || '').trim()

    if (sourcePath) {
        const resolved = path.resolve(sourcePath)
        if (!fs.existsSync(resolved)) {
            throw new Error(`Cookie source file not found: ${resolved}`)
        }
        const content = fs.readFileSync(resolved, 'utf8')
        if (resolved.toLowerCase().endsWith('.txt') || isProbablyNetscapeCookie(content)) {
            return {
                document: buildCookieDocumentFromNetscape(resolved),
                sourcePath: resolved,
            }
        }
        return {
            document: normalizeBiliupCookieDocument(JSON.parse(content)),
            sourcePath: resolved,
        }
    }

    if (uploadTarget.uploadConfig.cookie_file && uploadTarget.uploadConfig.browser_cookie_sync) {
        try {
            await runBrowserCookieSync(uploadTarget.uploadConfig, log)
        } catch (error) {
            log?.warn(`Archive upload browser cookie sync failed: ${error instanceof Error ? error.message : String(error)}`)
        }
    }

    if (uploadTarget.uploadConfig.cookie_file && fs.existsSync(uploadTarget.uploadConfig.cookie_file)) {
        return {
            document: normalizeBiliupCookieDocument(JSON.parse(fs.readFileSync(uploadTarget.uploadConfig.cookie_file, 'utf8'))),
            sourcePath: uploadTarget.uploadConfig.cookie_file,
        }
    }

    if (uploadTarget.sessdata && uploadTarget.bili_jct) {
        return {
            document: buildCookieDocument(uploadTarget.sessdata, uploadTarget.bili_jct),
            sourcePath: '[config:sessdata+bili_jct]',
        }
    }

    throw new Error('No usable Bilibili cookie source found for archive upload')
}

function createWaveformCachePath(item: ResolvedArchiveItem) {
    const size = item.remote ? item.sizeBytes : fs.statSync(item.mediaPath).size
    const mtimeMs = item.remote ? Date.parse(item.modifiedAt) || 0 : fs.statSync(item.mediaPath).mtimeMs
    const key = hashText(
        JSON.stringify({
            mediaPath: item.mediaPath,
            size,
            mtimeMs,
            width: WAVEFORM_WIDTH,
            height: WAVEFORM_HEIGHT,
        }),
    )
    return path.join(ensureDirectory(path.join(CACHE_DIR_ROOT, 'archive-admin', 'waveforms')), `${key}.png`)
}

function ensureWaveformImage(item: ResolvedArchiveItem, config: AppConfig) {
    const cachePath = createWaveformCachePath(item)
    if (fs.existsSync(cachePath) && fs.statSync(cachePath).size > 0) {
        return cachePath
    }

    const uploadTarget = resolveAdminUploadTarget(config)
    const mediaPath = getArchiveAccessPath(item)
    const ffmpegPath = resolveFfmpegPath(uploadTarget.uploadConfig)
    try {
        execFileSync(
            ffmpegPath,
            [
                '-y',
                '-i',
                mediaPath,
                '-filter_complex',
                `[0:a:0]aformat=channel_layouts=mono,showwavespic=s=${WAVEFORM_WIDTH}x${WAVEFORM_HEIGHT}:colors=0x3b82f6[waveform]`,
                '-map',
                '[waveform]',
                '-frames:v',
                '1',
                '-vcodec',
                'png',
                cachePath,
            ],
            {
                stdio: ['ignore', 'ignore', 'pipe'],
            },
        )
    } catch {
        // Some captures may be video-only or have unusual audio layouts. Keep the
        // admin page usable by falling back to a neutral placeholder image.
        execFileSync(
            ffmpegPath,
            [
                '-y',
                '-f',
                'lavfi',
                '-i',
                `color=c=0x0f172a:s=${WAVEFORM_WIDTH}x${WAVEFORM_HEIGHT}`,
                '-frames:v',
                '1',
                '-vcodec',
                'png',
                cachePath,
            ],
            {
                stdio: ['ignore', 'ignore', 'pipe'],
            },
        )
    }
    return cachePath
}

function buildFrameTimes(durationSeconds: number | null, count: number, trimStartSeconds: number, trimEndSeconds: number) {
    const safeCount = Math.max(1, Math.min(12, Math.floor(count || DEFAULT_FRAME_COUNT)))
    if (!durationSeconds || !Number.isFinite(durationSeconds) || durationSeconds <= 0) {
        return Array.from({ length: safeCount }, (_, index) => 2 + index * 4)
    }

    const start = Math.max(0, trimStartSeconds)
    const end = Math.max(start + 1, durationSeconds - Math.max(0, trimEndSeconds))
    const span = Math.max(1, end - start)
    return Array.from({ length: safeCount }, (_, index) => start + (span * (index + 1)) / (safeCount + 1))
}

function clampTimeSeconds(value: number, durationSeconds: number | null) {
    const numeric = Number(value)
    if (!Number.isFinite(numeric)) {
        return 0
    }
    if (!durationSeconds || !Number.isFinite(durationSeconds) || durationSeconds <= 0) {
        return Math.max(0, numeric)
    }
    return Math.max(0, Math.min(durationSeconds, numeric))
}

function normalizeExplicitFrameTimes(times: Array<number>, durationSeconds: number | null) {
    return Array.from(
        new Set(
            times
                .map((time) => clampTimeSeconds(time, durationSeconds))
                .filter((time) => Number.isFinite(time))
                .map((time) => Number(time.toFixed(6))),
        ),
    ).sort((left, right) => left - right)
}

function collectKeyFrameTimes(
    filePath: string,
    rangeStartSeconds: number,
    rangeEndSeconds: number,
    limit = 160,
) {
    const safeStart = Math.max(0, Number(rangeStartSeconds || 0))
    const safeEnd = Math.max(safeStart, Number(rangeEndSeconds || safeStart))
    const span = Math.max(0.25, safeEnd - safeStart)
    try {
        const output = execFileSync(
            resolveFfprobePath(),
            [
                '-v',
                'error',
                '-select_streams',
                'v:0',
                '-skip_frame',
                'nokey',
                '-show_frames',
                '-show_entries',
                'frame=best_effort_timestamp_time,pkt_pts_time,pkt_dts_time',
                '-of',
                'json',
                '-read_intervals',
                `${safeStart}%+${span}`,
                filePath,
            ],
            {
                encoding: 'utf8',
                maxBuffer: 8 * 1024 * 1024,
            },
        )
        const payload = safeJsonParse<{
            frames?: Array<{
                best_effort_timestamp_time?: string
                pkt_pts_time?: string
                pkt_dts_time?: string
            }>
        }>(output)

        return Array.from(
            new Set(
                (payload?.frames || [])
                    .map((frame) =>
                        Number(
                            frame.best_effort_timestamp_time
                            || frame.pkt_pts_time
                            || frame.pkt_dts_time
                            || 0,
                        ),
                    )
                    .filter((time) => Number.isFinite(time) && time >= safeStart && time <= safeEnd)
                    .map((time) => Number(time.toFixed(6))),
            ),
        )
            .sort((left, right) => left - right)
            .slice(0, limit)
    } catch {
        return []
    }
}

function captureFrameDataUrl(
    filePath: string,
    ffmpegPath: string,
    timeSeconds: number,
) {
    const output = execFileSync(
        ffmpegPath,
        [
            '-y',
            '-ss',
            `${Math.max(0, timeSeconds)}`,
            '-i',
            filePath,
            '-frames:v',
            '1',
            '-vf',
            'scale=640:-2',
            '-f',
            'image2pipe',
            '-vcodec',
            'mjpeg',
            'pipe:1',
        ],
        {
            maxBuffer: 8 * 1024 * 1024,
            stdio: ['ignore', 'pipe', 'pipe'],
        },
    ) as Buffer
    return `data:image/jpeg;base64,${output.toString('base64')}`
}

function generateFramePreviews(
    item: ResolvedArchiveItem,
    config: AppConfig,
    options: {
        count: number
        trimStartSeconds: number
        trimEndSeconds: number
        times?: Array<number>
        anchorTimeSeconds?: number | null
        includeKeyframes?: boolean
        keyframeRangeStartSeconds?: number | null
        keyframeRangeEndSeconds?: number | null
    },
): ArchiveFramePreviewBatch {
    const uploadTarget = resolveAdminUploadTarget(config)
    const ffmpegPath = resolveFfmpegPath(uploadTarget.uploadConfig)
    const mediaPath = getArchiveAccessPath(item)
    const mediaDetails = probeMediaDetails(mediaPath)
    const explicitTimes = normalizeExplicitFrameTimes(options.times || [], mediaDetails.durationSeconds)
    const times = explicitTimes.length > 0
        ? explicitTimes
        : buildFrameTimes(
            mediaDetails.durationSeconds,
            options.count,
            options.trimStartSeconds,
            options.trimEndSeconds,
        )
    const keyframeRangeStart = clampTimeSeconds(
        Number(
            options.keyframeRangeStartSeconds
            ?? (times.length ? times[0] : 0),
        ),
        mediaDetails.durationSeconds,
    )
    const keyframeRangeEnd = clampTimeSeconds(
        Number(
            options.keyframeRangeEndSeconds
            ?? (times.length ? times[times.length - 1] : keyframeRangeStart),
        ),
        mediaDetails.durationSeconds,
    )

    return {
        frames: times.map((timeSeconds) => ({
            timeSeconds,
            dataUrl: captureFrameDataUrl(mediaPath, ffmpegPath, timeSeconds),
        })),
        frameRate: mediaDetails.frameRate,
        anchorTimeSeconds: options.anchorTimeSeconds ?? (times[Math.floor(times.length / 2)] ?? null),
        keyFrameTimes: options.includeKeyframes
            ? collectKeyFrameTimes(mediaPath, keyframeRangeStart, keyframeRangeEnd)
            : [],
    }
}

function extractFrameToPath(
    filePath: string,
    ffmpegPath: string,
    timeSeconds: number,
    outputPath: string,
) {
    execFileSync(
        ffmpegPath,
        [
            '-y',
            '-ss',
            `${Math.max(0, timeSeconds)}`,
            '-i',
            filePath,
            '-frames:v',
            '1',
            '-q:v',
            '2',
            outputPath,
        ],
        {
            stdio: ['ignore', 'ignore', 'pipe'],
        },
    )
    return outputPath
}

function createUploadWorkspace(title: string) {
    const uploadRoot = ensureDirectory(path.join(CACHE_DIR_ROOT, 'archive-admin', 'uploads'))
    const prefix = sanitizeSegment(title, 'archive').slice(0, 32)
    return fs.mkdtempSync(path.join(uploadRoot, `${prefix}-`))
}

function trimArchiveMedia(
    inputPath: string,
    ffmpegPath: string,
    outputDir: string,
    trimStartSeconds: number,
    trimEndSeconds: number,
    durationSeconds: number | null,
) {
    const start = Math.max(0, trimStartSeconds)
    const end = durationSeconds && durationSeconds > 0
        ? Math.max(start + 1, durationSeconds - Math.max(0, trimEndSeconds))
        : null

    if (start <= 0 && (!end || !durationSeconds || end >= durationSeconds)) {
        return {
            path: inputPath,
            trimmed: false,
        }
    }

    const extension = normalizeExtension(inputPath) || '.mp4'
    const outputPath = path.join(outputDir, `trimmed${extension}`)

    const copyArgs = ['-y']
    if (start > 0) {
        copyArgs.push('-ss', `${start}`)
    }
    copyArgs.push('-i', inputPath)
    if (end && Number.isFinite(end)) {
        copyArgs.push('-to', `${end}`)
    }
    copyArgs.push('-avoid_negative_ts', 'make_zero', '-c', 'copy', outputPath)

    try {
        execFileSync(ffmpegPath, copyArgs, {
            stdio: ['ignore', 'ignore', 'pipe'],
        })
        if (fs.existsSync(outputPath) && fs.statSync(outputPath).size > 0) {
            return {
                path: outputPath,
                trimmed: true,
            }
        }
    } catch { }

    const reencodePath = path.join(outputDir, 'trimmed.mp4')
    const reencodeArgs = ['-y']
    if (start > 0) {
        reencodeArgs.push('-ss', `${start}`)
    }
    reencodeArgs.push('-i', inputPath)
    if (end && Number.isFinite(end)) {
        reencodeArgs.push('-to', `${end}`)
    }
    reencodeArgs.push('-c:v', 'libx264', '-preset', 'veryfast', '-crf', '20', '-c:a', 'aac', '-b:a', '192k', reencodePath)
    execFileSync(ffmpegPath, reencodeArgs, {
        stdio: ['ignore', 'ignore', 'pipe'],
    })
    return {
        path: reencodePath,
        trimmed: true,
    }
}

function normalizeTags(tags: Array<string> | string | undefined, fallbackTags: Array<string>) {
    if (Array.isArray(tags)) {
        return uniqueStrings(tags.map((tag) => String(tag).trim()))
    }
    if (typeof tags === 'string') {
        return uniqueStrings(tags.split(',').map((tag) => tag.trim()))
    }
    return [...fallbackTags]
}

function parseJsonTail(value: string) {
    const cleaned = String(value || '')
    const lines = cleaned
        .split(/\r?\n/)
        .map((line) => line.trim())
        .filter(Boolean)

    for (let index = lines.length - 1; index >= 0; index -= 1) {
        const line = lines[index] || ''
        if (!line.startsWith('{')) {
            continue
        }
        const parsed = safeJsonParse<Record<string, unknown>>(line)
        if (parsed && !Array.isArray(parsed)) {
            return parsed
        }
    }

    for (let index = cleaned.length - 1; index >= 0; index -= 1) {
        if (cleaned[index] !== '{') {
            continue
        }
        const parsed = safeJsonParse<Record<string, unknown>>(cleaned.slice(index))
        if (parsed && !Array.isArray(parsed)) {
            return parsed
        }
    }

    return null
}

function extractSubmitIdentifiers(rawOutput: string) {
    const payload = parseJsonTail(rawOutput)
    if (!payload) {
        return {
            aid: null,
            bvid: null,
        }
    }

    const submitResult = (payload.submit_result || payload) as Record<string, unknown>
    const nested = typeof submitResult.data === 'object' && submitResult.data ? (submitResult.data as Record<string, unknown>) : null
    for (const candidate of [submitResult, nested].filter(Boolean) as Array<Record<string, unknown>>) {
        const aid = String(candidate.aid || candidate.avid || '').trim()
        const bvid = String(candidate.bvid || candidate.bv_id || '').trim()
        if (aid || bvid) {
            return {
                aid: aid || null,
                bvid: bvid || null,
            }
        }
    }

    return {
        aid: null,
        bvid: null,
    }
}

async function runArchiveBiliupUpload(
    uploadConfig: ResolvedBiliupVideoUploadConfig,
    cookieFilePath: string,
    request: {
        title: string
        description: string
        sourceUrl: string
        tags: Array<string>
        tid: number
        threads: number
        submitApi: string
        line: string
        copyright: number
        videoPath: string
        coverPath?: string | null
        workDir: string
    },
    log?: Logger,
) {
    if (!fs.existsSync(uploadConfig.helper_path)) {
        throw new Error(`biliup helper not found: ${uploadConfig.helper_path}`)
    }

    const args = [
        uploadConfig.helper_path,
        '--cookie-file',
        cookieFilePath,
        '--title',
        request.title,
        '--desc',
        request.description,
        '--source-url',
        request.sourceUrl,
        '--tid',
        String(request.tid),
        '--threads',
        String(request.threads),
        '--submit-api',
        request.submitApi,
        '--line',
        request.line,
        '--copyright',
        String(request.copyright),
    ]

    if (request.coverPath) {
        args.push('--cover', request.coverPath)
    }

    for (const tag of request.tags) {
        args.push('--tag', tag)
    }

    args.push('--', request.videoPath)

    const stdoutChunks: Array<string> = []
    const stderrChunks: Array<string> = []

    await new Promise<void>((resolve, reject) => {
        const child = spawn(uploadConfig.python_path, args, {
            cwd: request.workDir,
            env: {
                ...process.env,
                PYTHONUNBUFFERED: '1',
            },
        })

        child.stdout.on('data', (chunk) => {
            const text = chunk.toString()
            stdoutChunks.push(text)
            text.trim() && log?.debug(`[archive-biliup] ${text.trim()}`)
        })
        child.stderr.on('data', (chunk) => {
            const text = chunk.toString()
            stderrChunks.push(text)
            text.trim() && log?.warn(`[archive-biliup] ${text.trim()}`)
        })
        child.on('error', (error) => reject(error))
        child.on('close', (code) => {
            if (code === 0) {
                resolve()
                return
            }
            reject(new Error(stderrChunks.join('').trim() || stdoutChunks.join('').trim() || `biliup exited with code ${code}`))
        })
    })

    return stdoutChunks.join('')
}

function listArchives(config: AppConfig, options?: { limit?: number; query?: string }) {
    const limit = Math.max(1, Math.min(300, Math.floor(options?.limit || DEFAULT_ARCHIVE_LIST_LIMIT)))
    const query = String(options?.query || '').trim().toLowerCase()
    const items = getResolvedArchives()
        .filter((item) => {
            if (!query) {
                return true
            }
            return [
                item.title,
                item.fileName,
                item.localPath,
                item.session?.page_url,
                item.session?.source,
            ]
                .filter(Boolean)
                .some((value) => String(value).toLowerCase().includes(query))
        })
        .slice(0, limit)
        .map((item) => summarizeArchive(item))

    return {
        items,
        defaults: buildArchiveUploadDefaults(config),
    }
}

function getArchiveDetail(config: AppConfig, archiveId: string): ArchiveDetail {
    const item = findArchiveOrThrow(archiveId)
    const mediaDetails = probeMediaDetails(getArchiveAccessPath(item))
    return {
        ...summarizeArchive(item),
        durationSeconds: mediaDetails.durationSeconds,
        frameRate: mediaDetails.frameRate,
        relatedFiles: resolveArchiveRelatedFiles(item),
        session: item.session,
        suggestedUpload: buildSuggestedUpload(item, config),
    }
}

function getArchiveDownloadFile(archiveId: string) {
    const item = findArchiveOrThrow(archiveId)
    const contentType = item.fileExtension === '.mp4'
        ? 'video/mp4'
        : item.fileExtension === '.mkv'
            ? 'video/x-matroska'
            : item.fileExtension === '.webm'
                ? 'video/webm'
                : item.fileExtension === '.mp3'
                    ? 'audio/mpeg'
                    : item.fileExtension === '.m4a'
                        ? 'audio/mp4'
                        : item.fileExtension === '.wav'
                            ? 'audio/wav'
                            : 'video/mp2t'
    return {
        filePath: getArchiveAccessPath(item),
        fileName: item.fileName,
        contentType,
    }
}

function getArchiveWaveformFile(config: AppConfig, archiveId: string) {
    const item = findArchiveOrThrow(archiveId)
    return ensureWaveformImage(item, config)
}

function getArchiveFramePreviews(
    config: AppConfig,
    archiveId: string,
    options?: {
        count?: number
        trimStartSeconds?: number
        trimEndSeconds?: number
        times?: Array<number>
        anchorTimeSeconds?: number | null
        includeKeyframes?: boolean
        keyframeRangeStartSeconds?: number | null
        keyframeRangeEndSeconds?: number | null
    },
) {
    const item = findArchiveOrThrow(archiveId)
    return generateFramePreviews(
        item,
        config,
        {
            count: Math.floor(options?.count || DEFAULT_FRAME_COUNT),
            trimStartSeconds: Math.max(0, Number(options?.trimStartSeconds || 0)),
            trimEndSeconds: Math.max(0, Number(options?.trimEndSeconds || 0)),
            times: options?.times,
            anchorTimeSeconds: options?.anchorTimeSeconds ?? null,
            includeKeyframes: Boolean(options?.includeKeyframes),
            keyframeRangeStartSeconds: options?.keyframeRangeStartSeconds ?? null,
            keyframeRangeEndSeconds: options?.keyframeRangeEndSeconds ?? null,
        },
    )
}

async function uploadArchiveToBilibili(
    config: AppConfig,
    archiveId: string,
    payload: ArchiveUploadRequest,
    log?: Logger,
): Promise<ArchiveUploadResult> {
    const item = findArchiveOrThrow(archiveId)
    const defaults = buildSuggestedUpload(item, config)
    const uploadTarget = resolveAdminUploadTarget(config)
    const ffmpegPath = resolveFfmpegPath(uploadTarget.uploadConfig)
    const archiveMediaPath = getArchiveAccessPath(item)
    const mediaDetails = probeMediaDetails(archiveMediaPath)

    const title = sanitizeSegment(payload.title || defaults.title, defaults.title).slice(0, 80)
    const description = String(payload.description || defaults.description || '').trim()
    const sourceUrl = String(payload.sourceUrl || defaults.sourceUrl || 'https://tv.n2nj.moe').trim()
    const tags = normalizeTags(payload.tags, defaults.tags)
    const tid = Math.max(1, Math.floor(Number(payload.tid || defaults.tid)))
    const threads = Math.max(1, Math.floor(Number(payload.threads || defaults.threads)))
    const submitApi = String(payload.submitApi || defaults.submitApi || 'web').trim() || 'web'
    const line = String(payload.line || defaults.line || 'AUTO').trim() || 'AUTO'
    const copyright = Number(payload.copyright || defaults.copyright || 2) === 1 ? 1 : 2
    const trimStartSeconds = Math.max(0, Number(payload.trimStartSeconds || 0))
    const trimEndSeconds = Math.max(0, Number(payload.trimEndSeconds || 0))

    if (mediaDetails.durationSeconds && trimStartSeconds + trimEndSeconds >= mediaDetails.durationSeconds - 1) {
        throw new Error('Trim start and end exceed the archive duration')
    }

    const workDir = createUploadWorkspace(title)
    const cookie = await loadCookieDocument(config, String(payload.cookieSourcePath || defaults.cookieSourcePath || ''), log)
    const cookieFilePath = path.join(workDir, 'cookies.json')
    fs.writeFileSync(cookieFilePath, JSON.stringify(cookie.document, null, 2), 'utf8')

    const trimmed = trimArchiveMedia(
        archiveMediaPath,
        ffmpegPath,
        workDir,
        trimStartSeconds,
        trimEndSeconds,
        mediaDetails.durationSeconds,
    )

    let coverPath: string | null = null
    if (payload.coverTimeSeconds !== null && payload.coverTimeSeconds !== undefined) {
        coverPath = extractFrameToPath(
            trimmed.path,
            ffmpegPath,
            Math.max(0, Number(payload.coverTimeSeconds || 0)),
            path.join(workDir, 'cover.jpg'),
        )
    }

    const stdout = await runArchiveBiliupUpload(
        uploadTarget.uploadConfig,
        cookieFilePath,
        {
            title,
            description,
            sourceUrl,
            tags,
            tid,
            threads,
            submitApi,
            line,
            copyright,
            videoPath: trimmed.path,
            coverPath,
            workDir,
        },
        log,
    )

    const identifiers = extractSubmitIdentifiers(stdout)
    return {
        ok: true,
        title,
        sourceUrl,
        cookieSourcePath: cookie.sourcePath,
        uploadedPath: archiveMediaPath,
        trimmedPath: trimmed.trimmed ? trimmed.path : null,
        coverPath,
        bvid: identifiers.bvid,
        aid: identifiers.aid,
        videoUrl: identifiers.bvid ? `https://www.bilibili.com/video/${identifiers.bvid}` : null,
        stdout,
    }
}

export {
    getArchiveDetail,
    getArchiveDownloadFile,
    getArchiveFramePreviews,
    getArchiveWaveformFile,
    listArchives,
    uploadArchiveToBilibili,
}
export type {
    ArchiveDetail,
    ArchiveFramePreview,
    ArchiveSummary,
    ArchiveUploadDefaults,
    ArchiveUploadRequest,
    ArchiveUploadResult,
}
