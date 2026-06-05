import { expect, test } from 'bun:test'
import { mkdtempSync, rmSync, writeFileSync } from 'fs'
import { join } from 'path'
import { tmpdir } from 'os'
import { getCookieString, parseNetscapeCookieToPuppeteerCookie, SimpleExpiringCache } from '../src/utils'

function withCookieFile(content: string, run: (file: string) => void) {
    const dir = mkdtempSync(join(tmpdir(), 'idol-bbq-cookie-test-'))
    try {
        const file = join(dir, 'cookies.txt')
        writeFileSync(file, content)
        run(file)
    } finally {
        rmSync(dir, { recursive: true, force: true })
    }
}

test('Netscape cookie parser skips comments, malformed rows, and expired cookies by default', () => {
    withCookieFile(
        [
            '# Netscape HTTP Cookie File',
            '.example.com\tTRUE\t/\tTRUE\t1000\texpired\told',
            '.example.com\tTRUE\t/\tTRUE\t3000\tvalid\tnew',
            'malformed row',
            '',
        ].join('\n'),
        (file) => {
            const cookies = parseNetscapeCookieToPuppeteerCookie(file, { now: 2000 })

            expect(cookies).toEqual([
                {
                    name: 'valid',
                    value: 'new',
                    domain: '.example.com',
                    path: '/',
                    expires: 3000,
                    httpOnly: false,
                    secure: true,
                },
            ])
            expect(getCookieString(cookies)).toBe('valid=new')
        },
    )
})

test('Netscape cookie parser preserves HttpOnly cookies and can include expired rows for audits', () => {
    withCookieFile(
        [
            '#HttpOnly_.x.com\tTRUE\t/\tTRUE\t1000\tct0\tcsrf-token',
            '.x.com TRUE / FALSE 3000 auth_token auth value with spaces',
        ].join('\n'),
        (file) => {
            const cookies = parseNetscapeCookieToPuppeteerCookie(file, { includeExpired: true, now: 2000 })

            expect(cookies).toEqual([
                {
                    name: 'ct0',
                    value: 'csrf-token',
                    domain: '.x.com',
                    path: '/',
                    expires: 1000,
                    httpOnly: true,
                    secure: true,
                },
                {
                    name: 'auth_token',
                    value: 'auth value with spaces',
                    domain: '.x.com',
                    path: '/',
                    expires: 3000,
                    httpOnly: false,
                    secure: false,
                },
            ])
        },
    )
})

test('SimpleExpiringCache treats ttl as seconds', async () => {
    const cache = new SimpleExpiringCache()

    cache.set('short', 'value', 0.01)
    expect(cache.get('short')).toBe('value')
    await new Promise((resolve) => setTimeout(resolve, 20))

    expect(cache.get('short')).toBeNull()
})
