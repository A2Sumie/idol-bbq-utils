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
    /**
     * Built-in deterministic processor for rule-based extraction/merge
     */
    Mechanical = 'Mechanical',
}

interface ProcessorConfig extends CommonCfgConfig {
    action?: 'translate' | 'extract' | 'merge' | 'plan'
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
    max_tokens?: number
    temperature?: number
    /**
     * extra config for request body
     */
    extended_payload?: Record<string, any>
    output_schema?: Record<string, any>
    schedule_url?: string
    schedule_api_key?: string
    result_key?: string
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
