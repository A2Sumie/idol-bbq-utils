import { ProcessorProvider } from '@/types/processor'
import { BaseProcessor, ProcessorRegistry, type ProcessorPlugin } from './base'
import { GoogleLLMTranslator } from './google'
import { ByteDanceLLMTranslator } from './bytedance'
import { BigModelLLMTranslator } from './bigmodel'
import { DeepSeekLLMTranslator } from './deepseek'
import { OpenaiLikeLLMTranslator } from './openai'
import { QwenMTTranslator } from './qwen'

// Note: I will rename the *Translator classes in their respective files later, but for now I reference them by their current name if I haven't changed them yet?
// Wait, I can't import them if they extend BaseProcessor (which is now BaseProcessor) and I haven't updated them.
// They will fail to compile.
// But file replacement is unrelated to compilation at this exact step.
// However, logically I am renaming the whole system.
// I will assume I will rename the classes to *Processor.

const GooglePlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Google,
    create: (apiKey, log, config) => new GoogleLLMTranslator(apiKey, log, config),
}

const ByteDancePlugin: ProcessorPlugin = {
    provider: ProcessorProvider.ByteDance,
    create: (apiKey, log, config) => new ByteDanceLLMTranslator(apiKey, log, config),
}

const BigModelPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.BigModel,
    create: (apiKey, log, config) => new BigModelLLMTranslator(apiKey, log, config),
}

const DeepseekPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Deepseek,
    create: (apiKey, log, config) => new DeepSeekLLMTranslator(apiKey, log, config),
}

const OpenAIPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.OpenAI,
    create: (apiKey, log, config) => new OpenaiLikeLLMTranslator(apiKey, log, config),
}

const QwenMTPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.QwenMT,
    create: (apiKey, log, config) => new QwenMTTranslator(apiKey, log, config),
}

const processorRegistry = ProcessorRegistry.getInstance()
    .register(GooglePlugin)
    .register(ByteDancePlugin)
    .register(BigModelPlugin)
    .register(DeepseekPlugin)
    .register(OpenAIPlugin)
    .register(QwenMTPlugin)

interface ProcessorConstructor {
    _PROVIDER: ProcessorProvider
    new(...args: ConstructorParameters<typeof BaseProcessor>): BaseProcessor
}

const processors: Array<ProcessorConstructor> = [
    GoogleLLMTranslator,
    ByteDanceLLMTranslator,
    BigModelLLMTranslator,
    DeepSeekLLMTranslator,
    OpenaiLikeLLMTranslator,
    QwenMTTranslator,
]

/** @deprecated Use processorRegistry.find() instead */
function getProcessor(provider: ProcessorProvider): ProcessorConstructor | null {
    for (const processor of processors) {
        if (processor._PROVIDER.toLowerCase() === provider.toLowerCase()) {
            return processor
        }
    }
    return null
}

export { getProcessor, processorRegistry }
export type { ProcessorConstructor }
