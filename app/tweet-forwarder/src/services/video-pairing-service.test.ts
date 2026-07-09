import { expect, test } from 'bun:test'
import { Platform } from '@idol-bbq-utils/spider/types'
import DB from '@/db'
import fs from 'fs'
import os from 'os'
import path from 'path'
import {
    BILIBILI_VIDEO_PAIRING_HELD_MODE,
    holdBilibiliVideoPairingTeaser,
    isBilibiliVideoPairingHeldResult,
    resolveVideoPairingConfig,
} from './video-pairing-service'

test('resolveVideoPairingConfig defaults expiry to drop', () => {
    expect(
        resolveVideoPairingConfig({
            video_pairing: {
                enabled: true,
                join_platforms: ['tiktok', 'ig'],
                window_seconds: 5400,
            },
        } as any),
    ).toEqual({
        enabled: true,
        joinPlatforms: ['tiktok', 'instagram'],
        windowSeconds: 5400,
        onExpiry: 'drop',
    })
})

test('holdBilibiliVideoPairingTeaser records X teaser media for a TikTok join link', async () => {
    const originalUpsertPending = DB.VideoPairing.upsertPending
    const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), 'video-pairing-'))
    const videoPath = path.join(tempDir, 'teaser.mp4')
    fs.writeFileSync(videoPath, 'video')
    const calls: any[] = []

    ;(DB.VideoPairing as any).upsertPending = async (input: any) => {
        calls.push(input)
        return {
            created: true,
            record: {
                id: 1,
                status: DB.VideoPairing.STATUS.Pending,
                ...input,
            },
        }
    }

    try {
        const result = await holdBilibiliVideoPairingTeaser({
            targetId: 'bilibili-转帖',
            article: {
                id: 100,
                platform: Platform.X,
                a_id: '2068685300046700614',
                u_id: 'member_x',
                username: 'member',
                created_at: 1782048000,
                content: 'TikTok更新 https://www.tiktok.com/@member_tt/video/7653464242506616085',
                url: 'https://x.com/member/status/2068685300046700614',
            } as any,
            media: [
                {
                    media_type: 'video',
                    path: videoPath,
                    sourceArticleId: '2068685300046700614',
                    content_hash: 'hash-teaser',
                    duration_seconds: 7,
                },
            ],
            config: resolveVideoPairingConfig({ video_pairing: true } as any),
        })

        expect(result.held).toBeTrue()
        expect(calls[0]).toMatchObject({
            target_id: 'bilibili-转帖',
            source_article_key: '1:2068685300046700614',
            source_platform: '1',
            join_platform: 'tiktok',
            target_video_id: '7653464242506616085',
            target_u_id: 'member_tt',
        })
        expect(calls[0].teaser_media).toEqual([
            {
                media_type: 'video',
                path: videoPath,
                sourceArticleId: '2068685300046700614',
                sourceUserId: undefined,
                content_hash: 'hash-teaser',
                size_bytes: undefined,
                duration_seconds: 7,
                sourceUrl: undefined,
            },
        ])
    } finally {
        ;(DB.VideoPairing as any).upsertPending = originalUpsertPending
        fs.rmSync(tempDir, { recursive: true, force: true })
    }
})

test('isBilibiliVideoPairingHeldResult detects nested held provider results', () => {
    expect(isBilibiliVideoPairingHeldResult([{ ok: true, mode: BILIBILI_VIDEO_PAIRING_HELD_MODE }])).toBeTrue()
    expect(isBilibiliVideoPairingHeldResult([{ ok: true, mode: 'biliup' }])).toBeFalse()
})
