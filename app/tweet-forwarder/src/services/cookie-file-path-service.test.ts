import { expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'fs'
import { tmpdir } from 'os'
import { join } from 'path'
import { resolveConfiguredCookieFilePath } from './cookie-file-path-service'

test('resolveConfiguredCookieFilePath maps container /app cookie paths to local cwd when absent', () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-cookie-path-'))
    try {
        expect(resolveConfiguredCookieFilePath('/app/assets/cookies/x.cookies.txt', { cwd: dir })).toBe(
            join(dir, 'assets/cookies/x.cookies.txt'),
        )
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('resolveConfiguredCookieFilePath preserves absolute paths that already exist', () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-cookie-path-'))
    try {
        const cookieFile = join(dir, 'cookies.txt')
        writeFileSync(cookieFile, 'cookie')
        expect(resolveConfiguredCookieFilePath(cookieFile, { cwd: join(dir, 'other') })).toBe(cookieFile)
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})

test('resolveConfiguredCookieFilePath resolves relative paths from config file directory', () => {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-cookie-path-'))
    try {
        expect(resolveConfiguredCookieFilePath('cookies/x.txt', { configPath: join(dir, 'config.yaml') })).toBe(
            join(dir, 'cookies/x.txt'),
        )
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
})
