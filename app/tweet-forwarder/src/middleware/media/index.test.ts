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
    const args = buildYtDlpArgs(
        'https://www.youtube.com/shorts/NYnbjoDltqA',
        {},
        '/tmp/ytdlp/%(id)s.%(ext)s',
    )

    expect(args).toContain('-f')
    expect(args[args.indexOf('-f') + 1]).toBe(DEFAULT_YT_DLP_FORMAT)
})
