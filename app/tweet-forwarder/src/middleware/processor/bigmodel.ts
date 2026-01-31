import { BaseProcessor } from './base'
import axios from 'axios'
import { Logger } from '@idol-bbq-utils/log'
import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'

enum EnumBigModel {
    GLM4Flash = 'glm-4-flash',
}

abstract class BaseBigModel extends BaseProcessor {
    public name = 'base big model translator'
    protected BASE_URL = 'https://open.bigmodel.cn/api/paas/v4/chat/completions'
}

class BigModelLLMTranslator extends BaseBigModel {
    static _PROVIDER = ProcessorProvider.BigModel
    NAME: string
    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, config)
        this.NAME = this.config?.name || 'glm-4-flash'
        this.BASE_URL = this.config?.base_url || this.BASE_URL
    }
    public async process(text: string) {
        const res = await axios.post(
            this.BASE_URL,
            {
                max_tokens: 4000,
                ...this.config?.extended_payload,
                model: this.config?.model_id || EnumBigModel.GLM4Flash,
                messages: [
                    {
                        role: 'system',
                        content: this.config?.prompt || this.PROCESS_PROMPT,
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
        return res.data.choices[0].message.content
    }
}

export { BigModelLLMTranslator }
