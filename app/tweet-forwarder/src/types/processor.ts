import type { CommonCfgConfig } from './common'

type ByteDance_LLM = 'doubao-pro-128k'
type BigModel_LLM = 'glm-4-flash'
type Google_LLM = 'gemini'
type Deepseek_LLM = 'deepseek-v3'

type OpenA_Like_LLM = 'Openai'

enum ProcessorProvider {
    /**
     *
     */
    None = 'None',
    /**
     * default model id gemini-2.0-flash
     */
    Google = 'Google',
    /**
     * default model id glm-4-flash
     */
    BigModel = 'BigModel',
    /**
     * default model id doubao-pro-128k
     */
    ByteDance = 'ByteDance',
    /**
     * default model id deepseek-v3
     */
    Deepseek = 'Deepseek',
    /**
     * default model id openai
     */
    OpenAI = 'Openai',
    /**
     * Qwen MT model
     */
    QwenMT = 'QwenMT',
}

interface ProcessorConfig extends CommonCfgConfig {
    prompt?: string
    /**
     * Customize api url
     */
    base_url?: string
    /**
     * Name shown in logger
     */
    name?: string
    model_id?: string
    /**
     * extra config for request body
     */
    extended_payload?: Record<string, any>
}

interface Processor {
    id?: string
    name?: string
    group?: string
    provider: ProcessorProvider
    api_key: string
    cfg_processor?: ProcessorConfig
}

export { ProcessorProvider }
export type { Processor, ProcessorConfig }
