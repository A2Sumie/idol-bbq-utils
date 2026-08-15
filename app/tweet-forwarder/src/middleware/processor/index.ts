import { ProcessorProvider } from '@/types/processor'
import { BaseProcessor, ProcessorRegistry, type ProcessorPlugin } from './base'
import { GoogleLLMTranslator } from './google'
import { DeepSeekLLMTranslator } from './deepseek'
import {
    DeepSeekV4FlashTranslator,
    DeepSeekV4ProTranslator,
    Hy3FreeTranslator,
    OpenaiLikeLLMTranslator,
} from './openai'
import { MechanicalProcessor } from './mechanical'

const GooglePlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Google,
    create: (apiKey, log, config) => new GoogleLLMTranslator(apiKey, log, config),
}

const DeepseekPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Deepseek,
    create: (apiKey, log, config) => new DeepSeekLLMTranslator(apiKey, log, config),
}

const OpenAIPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.OpenAI,
    create: (apiKey, log, config) => new OpenaiLikeLLMTranslator(apiKey, log, config),
}

const DeepSeekV4FlashPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.DeepSeekV4Flash,
    aliases: ['V4Flash', 'DSV4Flash', 'DeepseekV4Flash', 'deepseek-v4-flash'],
    create: (apiKey, log, config) => new DeepSeekV4FlashTranslator(apiKey, log, config),
}

const DeepSeekV4ProPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.DeepSeekV4Pro,
    aliases: ['V4Pro', 'DSV4Pro', 'DeepseekV4Pro', 'deepseek-v4-pro', 'ds-v4pro'],
    create: (apiKey, log, config) => new DeepSeekV4ProTranslator(apiKey, log, config),
}

const Hy3FreePlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Hy3Free,
    aliases: ['Hy3Free', 'hy3-free', 'hy3', 'Hy3'],
    create: (apiKey, log, config) => new Hy3FreeTranslator(apiKey, log, config),
}

const MechanicalPlugin: ProcessorPlugin = {
    provider: ProcessorProvider.Mechanical,
    create: (apiKey, log, config) => new MechanicalProcessor(apiKey, log, config),
}

const processorRegistry = ProcessorRegistry.getInstance()
    .register(GooglePlugin)
    .register(DeepseekPlugin)
    .register(OpenAIPlugin)
    .register(DeepSeekV4FlashPlugin)
    .register(DeepSeekV4ProPlugin)
    .register(Hy3FreePlugin)
    .register(MechanicalPlugin)

interface ProcessorConstructor {
    _PROVIDER: ProcessorProvider
    new (...args: ConstructorParameters<typeof BaseProcessor>): BaseProcessor
}

const processors: Array<ProcessorConstructor> = [
    GoogleLLMTranslator,
    DeepSeekLLMTranslator,
    OpenaiLikeLLMTranslator,
    DeepSeekV4FlashTranslator,
    DeepSeekV4ProTranslator,
    Hy3FreeTranslator,
    MechanicalProcessor,
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
