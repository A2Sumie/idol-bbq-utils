import { expect, test } from 'bun:test'
import axios from 'axios'
import { processorRegistry } from '.'

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
