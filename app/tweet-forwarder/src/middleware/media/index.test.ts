import { test, expect } from 'bun:test'
import { buildYtDlpArgs, DEFAULT_YT_DLP_FORMAT } from './index'

test('buildYtDlpArgs includes cookies, output path, and custom format', () => {
    const args = buildYtDlpArgs(
        'https://www.youtube.com/watch?v=bBRUMp_WNUU',
        {
            cookie_file: '/app/assets/cookies/ycookies.txt',
            format: 'best[ext=mp4]',
        },
        '/tmp/ytdlp/%(id)s.%(ext)s',
    )

    expect(args).toEqual([
        '--no-playlist',
        '--no-progress',
        '--print',
        'after_move:filepath',
        '--merge-output-format',
        'mp4',
        '-o',
        '/tmp/ytdlp/%(id)s.%(ext)s',
        '--cookies',
        '/app/assets/cookies/ycookies.txt',
        '-f',
        'best[ext=mp4]',
        'https://www.youtube.com/watch?v=bBRUMp_WNUU',
    ])
})

test('buildYtDlpArgs falls back to the default mp4-first format', () => {
    const args = buildYtDlpArgs('https://www.youtube.com/shorts/NYnbjoDltqA', {}, '/tmp/ytdlp/%(id)s.%(ext)s')

    expect(args).toContain('-f')
    expect(args[args.indexOf('-f') + 1]).toBe(DEFAULT_YT_DLP_FORMAT)
})

test('buildYtDlpArgs includes pacing and retry controls', () => {
    const args = buildYtDlpArgs(
        'https://www.youtube.com/watch?v=bBRUMp_WNUU',
        {
            sleep_requests: 1,
            sleep_interval: 3,
            max_sleep_interval: 8,
            concurrent_fragments: 1,
            limit_rate: '2M',
            retry_sleep: 'exp=1:20',
        },
        '/tmp/ytdlp/%(id)s.%(ext)s',
    )

    expect(args).toContain('--sleep-requests')
    expect(args[args.indexOf('--sleep-requests') + 1]).toBe('1')
    expect(args[args.indexOf('--sleep-interval') + 1]).toBe('3')
    expect(args[args.indexOf('--max-sleep-interval') + 1]).toBe('8')
    expect(args[args.indexOf('--concurrent-fragments') + 1]).toBe('1')
    expect(args[args.indexOf('--limit-rate') + 1]).toBe('2M')
    expect(args[args.indexOf('--retry-sleep') + 1]).toBe('exp=1:20')
})
