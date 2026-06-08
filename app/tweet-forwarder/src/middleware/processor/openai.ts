import { BaseProcessor } from './base'
import axios from 'axios'
import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'
import { Logger } from '@idol-bbq-utils/log'

abstract class BaseOpenai extends BaseProcessor {
    public name = 'base openai translator'
    protected BASE_URL = 'https://api.openai.com/v1/chat/completions'
}

const DEEPSEEK_V4_FLASH_DEFAULT_CONFIG: ProcessorConfig = {
    name: 'OpenCode-Go-DeepSeek-v4-flash',
    model_id: 'deepseek-v4-flash',
    base_url: 'https://opencode.ai/zen/go/v1/chat/completions',
    temperature: 0.4,
    extended_payload: {
        thinking: {
            type: 'disabled',
        },
    },
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

export { DeepSeekV4FlashTranslator, OpenaiLikeLLMTranslator }
