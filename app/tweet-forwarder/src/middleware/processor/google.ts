import { ChatSession, GoogleGenerativeAI, HarmBlockThreshold, HarmCategory } from '@google/generative-ai'
import { BaseProcessor } from './base'
import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'
import { Logger } from '@idol-bbq-utils/log'

class GoogleLLMTranslator extends BaseProcessor {
    static _PROVIDER = ProcessorProvider.Google
    BASE_URL = ''
    NAME = 'Gemini'
    private genAI
    private model
    private prompt: string
    private chat: ChatSession | undefined
    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super(api_key, log, config)
        this.genAI = new GoogleGenerativeAI(api_key)
        const generationConfig: Record<string, unknown> = {}
        if (typeof this.config?.temperature === 'number') {
            generationConfig.temperature = this.config.temperature
        }
        if (typeof this.config?.max_tokens === 'number') {
            generationConfig.maxOutputTokens = this.config.max_tokens
        }
        if (this.config?.output_schema) {
            generationConfig.responseMimeType = 'application/json'
            generationConfig.responseSchema = this.config.output_schema
        }
        this.model = this.genAI.getGenerativeModel({
            model: this.config?.model_id || 'gemini-2.0-flash',
            safetySettings: [
                {
                    category: HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
                    threshold: HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    category: HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
                    threshold: HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    category: HarmCategory.HARM_CATEGORY_HATE_SPEECH,
                    threshold: HarmBlockThreshold.BLOCK_NONE,
                },
                {
                    category: HarmCategory.HARM_CATEGORY_HARASSMENT,
                    threshold: HarmBlockThreshold.BLOCK_NONE,
                },
            ],
            ...(Object.keys(generationConfig).length > 0
                ? { generationConfig: generationConfig as any }
                : {}),
        })
        this.prompt = this.getPrompt()
        this.NAME = config?.name || this.NAME
    }
    public async init() {
        await super.init()
        const chat = await this.model.startChat({
            history: [
                {
                    role: 'user',
                    parts: [{ text: this.prompt }],
                },
            ],
        })
        this.chat = chat
    }
    public async process(text: string) {
        const res = (await this.chat?.sendMessage(text))?.response.text() || ''
        return res
    }
}

export { GoogleLLMTranslator }
