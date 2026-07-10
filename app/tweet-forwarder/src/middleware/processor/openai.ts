import { BaseProcessor } from './base'
import axios from 'axios'
import { type ProcessorConfig, type ProcessorFallbackConfig, ProcessorProvider } from '@/types/processor'
import { Logger } from '@idol-bbq-utils/log'
import { getHy3CircuitBreaker } from '@/services/hy3-circuit-breaker-service'

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
    name: 'OpenCode-Zen-Hy3-Free',
    model_id: 'hy3-free',
    base_url: 'https://opencode.ai/zen/v1/chat/completions',
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
        const res = await axios.post(
            this.BASE_URL,
            {
                ...this.buildOpenAICompatibleRequestConfig(),
                ...this.config?.extended_payload,
                model: this.config?.model_id || 'openai',
                messages: [
                    {
                        role: 'system',
                        content: this.getPrompt(),
                    },
                    {
                        role: 'user',
                        content: text,
                    },
                ],
            },
            {
                headers: {
                    Authorization: `Bearer ${this.api_key}`,
                },
                timeout: this.config?.request_timeout_ms,
            },
        )
        return res.data.choices[0].message.content as string
    }
}

class DeepSeekV4FlashTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.DeepSeekV4Flash

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, mergeProcessorDefaults(DEEPSEEK_V4_FLASH_DEFAULT_CONFIG, config))
    }
}

class DeepSeekV4ProTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.DeepSeekV4Pro

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, mergeProcessorDefaults(DEEPSEEK_V4_PRO_DEFAULT_CONFIG, config))
    }
}

function buildFallbackProcessorConfig(
    primary: ProcessorConfig,
    fallback?: ProcessorFallbackConfig,
): ProcessorConfig {
    const { fallback: _omit, extended_payload: _primaryPayload, ...primaryShared } = primary
    const name = `${primary.name || 'hy3'}-fallback-v4pro`
    if (!fallback) {
        return { ...primaryShared, name }
    }
    return {
        ...primaryShared,
        name,
        model_id: fallback.model_id ?? primaryShared.model_id,
        base_url: fallback.base_url ?? primaryShared.base_url,
        temperature: fallback.temperature ?? primaryShared.temperature,
        extended_payload: fallback.extended_payload,
    }
}

class Hy3FreeTranslator extends OpenaiLikeLLMTranslator {
    static _PROVIDER = ProcessorProvider.Hy3Free
    private fallbackProcessor: DeepSeekV4ProTranslator
    private breaker = getHy3CircuitBreaker()

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        const merged = mergeProcessorDefaults(HY3_FREE_DEFAULT_CONFIG, config)
        super(api_key, log, merged)
        this.breaker = getHy3CircuitBreaker(log)
        const fallbackConfig = buildFallbackProcessorConfig(merged, config?.fallback)
        this.fallbackProcessor = new DeepSeekV4ProTranslator(api_key, log, fallbackConfig)
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
            this.log?.warn('HY3 frozen — using v4-pro fallback directly')
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
            this.log?.warn(`HY3 request failed — delegating to v4-pro fallback: ${error instanceof Error ? error.message : String(error)}`)
            return this.fallbackProcessor.process(text)
        }
    }
}

export { DeepSeekV4FlashTranslator, DeepSeekV4ProTranslator, Hy3FreeTranslator, OpenaiLikeLLMTranslator }
