import { expect, test } from 'bun:test'
import axios from 'axios'
import fs from 'fs'
import os from 'os'
import path from 'path'
import { processorRegistry } from '.'
import { resetHy3CircuitBreakerForTest } from '@/services/hy3-circuit-breaker-service'

function withHy3BreakerEnv<T>(threshold: string, fn: () => Promise<T>): Promise<T> {
    const statePath = path.join(os.tmpdir(), `hy3-breaker-openai-test-${Date.now()}-${Math.random().toString(36).slice(2)}.json`)
    const prevThreshold = process.env.HY3_FAILURE_THRESHOLD
    const prevStatePath = process.env.HY3_BREAKER_STATE_PATH
    process.env.HY3_FAILURE_THRESHOLD = threshold
    process.env.HY3_BREAKER_STATE_PATH = statePath
    resetHy3CircuitBreakerForTest()
    return fn().finally(() => {
        if (prevThreshold === undefined) delete process.env.HY3_FAILURE_THRESHOLD
        else process.env.HY3_FAILURE_THRESHOLD = prevThreshold
        if (prevStatePath === undefined) delete process.env.HY3_BREAKER_STATE_PATH
        else process.env.HY3_BREAKER_STATE_PATH = prevStatePath
        resetHy3CircuitBreakerForTest()
        try {
            if (fs.existsSync(statePath)) fs.unlinkSync(statePath)
        } catch {
            // ignore
        }
    })
}

test('DeepSeek V4 Pro provider applies OpenCode Go defaults and aliases', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        return {
            data: {
                choices: [
                    {
                        message: {
                            content: '译文',
                        },
                    },
                ],
            },
        }
    }

    try {
        const processor = await processorRegistry.create('ds-v4pro', 'test-key', undefined, {
            prompt: 'Translate to Simplified Chinese.',
        })
        const result = await processor.process('こんにちは')

        expect(result).toBe('译文')
        expect(calls).toHaveLength(1)
        expect(calls[0]?.url).toBe('https://opencode.ai/zen/go/v1/chat/completions')
        expect(calls[0]?.body).toMatchObject({
            model: 'deepseek-v4-pro',
            temperature: 1.0,
            thinking: {
                type: 'disabled',
            },
            messages: [
                {
                    role: 'system',
                    content: 'Translate to Simplified Chinese.',
                },
                {
                    role: 'user',
                    content: 'こんにちは',
                },
            ],
        })
        expect(calls[0]?.options?.headers?.Authorization).toBe('Bearer test-key')
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('DeepSeek V4 Flash provider applies OpenCode Go defaults and aliases', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        return {
            data: {
                choices: [
                    {
                        message: {
                            content: '译文',
                        },
                    },
                ],
            },
        }
    }

    try {
        const processor = await processorRegistry.create('V4Flash', 'test-key', undefined, {
            prompt: 'Translate to Simplified Chinese.',
        })
        const result = await processor.process('こんにちは')

        expect(result).toBe('译文')
        expect(calls).toHaveLength(1)
        expect(calls[0]?.url).toBe('https://opencode.ai/zen/go/v1/chat/completions')
        expect(calls[0]?.body).toMatchObject({
            model: 'deepseek-v4-flash',
            temperature: 1.3,
            thinking: {
                type: 'disabled',
            },
            messages: [
                {
                    role: 'system',
                    content: 'Translate to Simplified Chinese.',
                },
                {
                    role: 'user',
                    content: 'こんにちは',
                },
            ],
        })
        expect(calls[0]?.options?.headers?.Authorization).toBe('Bearer test-key')
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('DeepSeek V4 Flash provider can request JSON object mode without JSON schema', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        return {
            data: {
                choices: [
                    {
                        message: {
                            content: '{"items":[]}',
                        },
                    },
                ],
            },
        }
    }

    try {
        const processor = await processorRegistry.create('DeepSeekV4Flash', 'test-key', undefined, {
            prompt: 'Return JSON only.',
            response_format: 'json_object',
            output_schema: {
                type: 'object',
                properties: {
                    items: { type: 'array' },
                },
            },
        })
        const result = await processor.process('extract schedule')

        expect(result).toBe('{"items":[]}')
        expect(calls).toHaveLength(1)
        expect(calls[0]?.body.response_format).toEqual({ type: 'json_object' })
        expect(calls[0]?.body.response_format?.json_schema).toBeUndefined()
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('Hy3Free provider calls Tencent LKEAP endpoint with model hy3 on success', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        return { data: { choices: [{ message: { content: '译文' } }] } }
    }

    try {
        await withHy3BreakerEnv('10', async () => {
            const processor = await processorRegistry.create('hy3', 'test-key', undefined, {
                prompt: 'Translate to Simplified Chinese.',
                fallback: {
                    provider: 'DeepSeekV4Pro',
                    api_key: 'test-fallback-key',
                    model_id: 'deepseek-v4-pro',
                    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
                    temperature: 1.0,
                    extended_payload: { thinking: { type: 'disabled' } },
                },
            })
            const result = await processor.process('こんにちは')

            expect(result).toBe('译文')
            expect(calls).toHaveLength(1)
            expect(calls[0]?.url).toBe('https://api.lkeap.cloud.tencent.com/plan/v3/chat/completions')
            expect(calls[0]?.body).toMatchObject({
                model: 'hy3',
                temperature: 1.0,
                messages: [
                    { role: 'system', content: 'Translate to Simplified Chinese.' },
                    { role: 'user', content: 'こんにちは' },
                ],
            })
            expect(calls[0]?.body.thinking).toBeUndefined()
            expect(calls[0]?.options?.headers?.Authorization).toBe('Bearer test-key')
        })
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('Hy3Free provider falls back to v4-pro Go endpoint on failure', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        if (url.includes('lkeap.cloud.tencent.com')) {
            throw new Error('hy3 unavailable')
        }
        return { data: { choices: [{ message: { content: 'fallback译文' } }] } }
    }

    try {
        await withHy3BreakerEnv('10', async () => {
            const processor = await processorRegistry.create('Hy3Free', 'test-key', undefined, {
                prompt: 'Translate to Simplified Chinese.',
                fallback: {
                    provider: 'DeepSeekV4Pro',
                    api_key: 'test-fallback-key',
                    model_id: 'deepseek-v4-pro',
                    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
                    temperature: 1.0,
                    extended_payload: { thinking: { type: 'disabled' } },
                },
            })
            const result = await processor.process('こんにちは')

            expect(result).toBe('fallback译文')
            expect(calls).toHaveLength(2)
            expect(calls[0]?.url).toBe('https://api.lkeap.cloud.tencent.com/plan/v3/chat/completions')
            expect(calls[0]?.body.model).toBe('hy3')
            expect(calls[1]?.url).toBe('https://opencode.ai/zen/go/v1/chat/completions')
            expect(calls[1]?.body.model).toBe('deepseek-v4-pro')
            expect(calls[1]?.body).toMatchObject({ thinking: { type: 'disabled' } })
            expect(calls[1]?.body.messages).toMatchObject([
                { role: 'system', content: 'Translate to Simplified Chinese.' },
                { role: 'user', content: 'こんにちは' },
            ])
            expect(calls[1]?.options?.headers?.Authorization).toBe('Bearer test-fallback-key')
        })
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('Hy3Free fallback can use a separate api_key from the primary', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        if (url.includes('lkeap.cloud.tencent.com')) {
            throw new Error('hy3 unavailable')
        }
        return { data: { choices: [{ message: { content: 'fallback译文' } }] } }
    }

    try {
        await withHy3BreakerEnv('10', async () => {
            const processor = await processorRegistry.create('Hy3Free', 'tencent-key', undefined, {
                prompt: 'Translate to Simplified Chinese.',
                fallback: {
                    provider: 'DeepSeekV4Pro',
                    api_key: 'test-fallback-key',
                    api_key: 'go-key',
                    model_id: 'deepseek-v4-pro',
                    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
                    temperature: 1.0,
                    extended_payload: { thinking: { type: 'disabled' } },
                },
            })
            const result = await processor.process('こんにちは')

            expect(result).toBe('fallback译文')
            expect(calls[0]?.options?.headers?.Authorization).toBe('Bearer tencent-key')
            expect(calls[1]?.options?.headers?.Authorization).toBe('Bearer go-key')
        })
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('Hy3Free provider skips hy3 and goes straight to fallback when frozen', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        if (url.includes('lkeap.cloud.tencent.com')) {
            throw new Error('should not be called when frozen')
        }
        return { data: { choices: [{ message: { content: 'frozen-fallback译文' } }] } }
    }

    try {
        await withHy3BreakerEnv('2', async () => {
            const { getHy3CircuitBreaker, resolveHy3BreakerKey } = await import('@/services/hy3-circuit-breaker-service')
            const breaker = getHy3CircuitBreaker(undefined, resolveHy3BreakerKey({}))
            breaker.recordFailure(new Error('pre-freeze-1'))
            breaker.recordFailure(new Error('pre-freeze-2'))
            expect(breaker.isFrozen()).toBe(true)

            const processor = await processorRegistry.create('hy3', 'test-key', undefined, {
                prompt: 'Translate to Simplified Chinese.',
                fallback: {
                    provider: 'DeepSeekV4Pro',
                    api_key: 'test-fallback-key',
                    model_id: 'deepseek-v4-pro',
                    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
                    extended_payload: { thinking: { type: 'disabled' } },
                },
            })
            const result = await processor.process('こんにちは')

            expect(result).toBe('frozen-fallback译文')
            expect(calls).toHaveLength(1)
            expect(calls[0]?.url).toBe('https://opencode.ai/zen/go/v1/chat/completions')
            expect(calls[0]?.body.model).toBe('deepseek-v4-pro')
        })
    } finally {
        ;(axios as any).post = originalPost
    }
})

test('Hy3Free provider preserves prompt and response_format in fallback', async () => {
    const originalPost = axios.post
    const calls: Array<{ url: string; body: any; options: any }> = []
    ;(axios as any).post = async (url: string, body: any, options: any) => {
        calls.push({ url, body, options })
        if (url.includes('lkeap.cloud.tencent.com')) {
            throw new Error('hy3 unavailable')
        }
        return { data: { choices: [{ message: { content: '{"tags":["22/7"]}' } }] } }
    }

    try {
        await withHy3BreakerEnv('10', async () => {
            const processor = await processorRegistry.create('Hy3Free', 'test-key', undefined, {
                prompt: 'Return JSON only.',
                response_format: 'json_object',
                max_tokens: 160,
                fallback: {
                    provider: 'DeepSeekV4Pro',
                    api_key: 'test-fallback-key',
                    model_id: 'deepseek-v4-pro',
                    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
                    extended_payload: { thinking: { type: 'disabled' } },
                },
            })
            const result = await processor.process('generate tags')

            expect(result).toBe('{"tags":["22/7"]}')
            expect(calls[1]?.body.response_format).toEqual({ type: 'json_object' })
            expect(calls[1]?.body.max_tokens).toBe(160)
        })
    } finally {
        ;(axios as any).post = originalPost
    }
})
