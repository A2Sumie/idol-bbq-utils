import { BaseProcessor, resolveProcessorApiKey } from './base'
import axios from 'axios'
import { type ProcessorConfig, type ProcessorFallbackConfig, ProcessorProvider } from '@/types/processor'
import { Logger } from '@idol-bbq-utils/log'
import {
    getHy3CircuitBreaker,
    resolveHy3BreakerKey,
    type Hy3CircuitBreaker,
} from '@/services/hy3-circuit-breaker-service'

abstract class BaseOpenai extends BaseProcessor {
    public name = 'base openai translator'
    protected BASE_URL = 'https://api.openai.com/v1/chat/completions'
}

const DEEPSEEK_V4_FLASH_DEFAULT_CONFIG: ProcessorConfig = {
    name: 'OpenCode-Go-DeepSeek-v4-flash',
    model_id: 'deepseek-v4-flash',
    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
    temperature: 1.3,
    extended_payload: {
        thinking: {
            type: 'disabled',
        },
    },
}

const DEEPSEEK_V4_PRO_DEFAULT_CONFIG: ProcessorConfig = {
    name: 'OpenCode-Go-DeepSeek-v4-pro',
    model_id: 'deepseek-v4-pro',
    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
    temperature: 1.0,
    extended_payload: {
        thinking: {
            type: 'disabled',
        },
    },
}

const HY3_FREE_DEFAULT_CONFIG: ProcessorConfig = {
    name: 'Tencent-LKEAP-Hunyuan-Hy3',
    model_id: 'hy3',
    base_url: 'https://api.lkeap.cloud.tencent.com/plan/v3/chat/completions',
    temperature: 1.0,
}

function mergeProcessorDefaults(defaults: ProcessorConfig, config?: ProcessorConfig): ProcessorConfig {
    return {
        ...defaults,
        ...(config || {}),
        extended_payload: {
            ...(defaults.extended_payload || {}),
            ...(config?.extended_payload || {}),
        },
    }
}

class OpenaiLikeLLMTranslator extends BaseOpenai {
    static _PROVIDER = ProcessorProvider.OpenAI
    NAME: string
    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, config)
        this.api_key = api_key
        this.NAME = config?.name || 'Openai-like'
        this.BASE_URL = config?.base_url || this.BASE_URL
    }
    public async process(text: string) {
        const input = [
            {
                role: 'system',
                content: this.getPrompt(),
            },
            {
                role: 'user',
                content: text,
            },
        ]
        const compatibleConfig = this.buildOpenAICompatibleRequestConfig()
        const isResponsesApi = this.config?.wire_api === 'responses'
        const body = isResponsesApi
            ? this.buildResponsesRequest(input, compatibleConfig)
            : {
                  ...compatibleConfig,
                  ...this.config?.extended_payload,
                  model: this.config?.model_id || 'openai',
                  messages: input,
              }
        const res = await axios.post(this.BASE_URL, body, {
            headers: {
                Authorization: `Bearer ${this.api_key}`,
            },
            timeout: this.config?.request_timeout_ms ?? 30_000,
        })
        return isResponsesApi ? this.readResponsesText(res.data) : (res.data.choices[0].message.content as string)
    }

    private buildResponsesRequest(
        input: Array<{ role: string; content: string }>,
        compatibleConfig: Record<string, any>,
    ) {
        const { max_tokens, response_format, ...shared } = compatibleConfig
        return {
            ...shared,
            ...this.config?.extended_payload,
            model: this.config?.model_id || 'openai',
            input,
            ...(typeof max_tokens === 'number' ? { max_output_tokens: max_tokens } : {}),
            ...(response_format ? { text: { format: response_format } } : {}),
            ...(this.config?.reasoning_effort ? { reasoning: { effort: this.config.reasoning_effort } } : {}),
        }
    }

    private readResponsesText(data: any): string {
        const text = (Array.isArray(data?.output) ? data.output : [])
            .filter((item: any) => item?.type === 'message')
            .flatMap((item: any) => (Array.isArray(item?.content) ? item.content : []))
            .filter((item: any) => item?.type === 'output_text' && typeof item?.text === 'string')
            .map((item: any) => item.text)
            .join('')
        if (!text) {
            throw new Error('Responses API returned no output_text')
        }
        return text
    }
}

class DeepSeekV4FlashTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.DeepSeekV4Flash
    private fallbackProcessor: OpenaiLikeLLMTranslator | null = null

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, mergeProcessorDefaults(DEEPSEEK_V4_FLASH_DEFAULT_CONFIG, config))
        // Optional generic fallback (same shape as the Hy3Free breaker fallback):
        // a second wire/endpoint for the flash translator so a primary outage
        // does not leave articles untranslated.
        if (config?.fallback) {
            const fallbackConfig = buildFallbackProcessorConfig(this.config as ProcessorConfig, config.fallback)
            const fallbackApiKey = resolveProcessorApiKey(config.fallback.api_key)
            this.fallbackProcessor = new OpenaiLikeLLMTranslator(fallbackApiKey, log, fallbackConfig)
        }
    }

    async init(): Promise<void> {
        await super.init()
        await this.fallbackProcessor?.init()
    }

    async drop(...args: any[]): Promise<void> {
        await Promise.all([super.drop(...args), this.fallbackProcessor?.drop(...args)])
    }

    public async process(text: string): Promise<string> {
        try {
            return await super.process(text)
        } catch (error) {
            if (!this.fallbackProcessor) {
                throw error
            }
            this.log?.warn(
                `DeepSeekV4Flash primary failed; delegating to fallback processor: ${
                    error instanceof Error ? error.message : String(error)
                }`,
            )
            return await this.fallbackProcessor.process(text)
        }
    }
}

class DeepSeekV4ProTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.DeepSeekV4Pro

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, mergeProcessorDefaults(DEEPSEEK_V4_PRO_DEFAULT_CONFIG, config))
    }
}

function buildFallbackProcessorConfig(primary: ProcessorConfig, fallback?: ProcessorFallbackConfig): ProcessorConfig {
    const { fallback: _omit, extended_payload: _primaryPayload, ...primaryShared } = primary
    const name = `${primary.name || 'hunyuan'}-fallback`
    if (!fallback) {
        // Never default to the primary model/endpoint: that would re-POST the same request that just failed
        // and call it a "fallback". Fall back to the v4-pro defaults instead.
        return {
            ...primaryShared,
            name,
            model_id: DEEPSEEK_V4_PRO_DEFAULT_CONFIG.model_id,
            base_url: DEEPSEEK_V4_PRO_DEFAULT_CONFIG.base_url,
            temperature: DEEPSEEK_V4_PRO_DEFAULT_CONFIG.temperature,
        }
    }
    return {
        ...primaryShared,
        name,
        model_id: fallback.model_id ?? DEEPSEEK_V4_PRO_DEFAULT_CONFIG.model_id,
        base_url: fallback.base_url ?? DEEPSEEK_V4_PRO_DEFAULT_CONFIG.base_url,
        temperature: fallback.temperature ?? primaryShared.temperature,
        top_p: fallback.top_p ?? primaryShared.top_p,
        wire_api: fallback.wire_api ?? primaryShared.wire_api,
        reasoning_effort: fallback.reasoning_effort ?? primaryShared.reasoning_effort,
        extended_payload: fallback.extended_payload,
    }
}

class Hy3FreeTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.Hy3Free
    private fallbackProcessor: DeepSeekV4ProTranslator
    private breaker: Hy3CircuitBreaker

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        const merged = mergeProcessorDefaults(HY3_FREE_DEFAULT_CONFIG, config)
        super(api_key, log, merged)
        this.breaker = getHy3CircuitBreaker(log, resolveHy3BreakerKey(merged))
        const fallbackConfig = buildFallbackProcessorConfig(merged, config?.fallback)
        if (!config?.fallback?.api_key) {
            throw new Error(
                `Hy3Free processor "${merged.name}" requires cfg_processor.fallback.api_key: without a dedicated ` +
                    `fallback key the breaker would route failures back to the same endpoint that just failed.`,
            )
        }
        const fallbackApiKey = resolveProcessorApiKey(config.fallback.api_key)
        this.fallbackProcessor = new DeepSeekV4ProTranslator(fallbackApiKey, log, fallbackConfig)
    }

    async init(): Promise<void> {
        await super.init()
        await this.fallbackProcessor.init()
    }

    async drop(...args: any[]): Promise<void> {
        await Promise.all([super.drop(...args), this.fallbackProcessor.drop(...args)])
    }

    public async process(text: string): Promise<string> {
        if (this.breaker.isFrozen()) {
            this.log?.warn('HY3 frozen — using fallback processor directly')
            this.breaker.recordFallback()
            return this.fallbackProcessor.process(text)
        }
        try {
            const result = await super.process(text)
            this.breaker.recordSuccess()
            return result
        } catch (error) {
            this.breaker.recordFailure(error)
            this.breaker.recordFallback()
            this.log?.warn(
                `HY3 request failed — delegating to fallback processor: ${error instanceof Error ? error.message : String(error)}`,
            )
            return this.fallbackProcessor.process(text)
        }
    }
}

export { DeepSeekV4FlashTranslator, DeepSeekV4ProTranslator, Hy3FreeTranslator, OpenaiLikeLLMTranslator }
