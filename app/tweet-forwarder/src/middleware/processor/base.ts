import { type ProcessorConfig, ProcessorProvider } from '@/types/processor'
import { BaseCompatibleModel } from '@/utils/base'
import { Logger } from '@idol-bbq-utils/log'
import { noop } from 'lodash'

const PROCESSOR_ERROR_FALLBACK = '╮(╯-╰)╭非常抱歉无法处理'

abstract class BaseProcessor extends BaseCompatibleModel {
    static _PROVIDER = ProcessorProvider.None
    protected abstract BASE_URL: string
    protected api_key: string
    log?: Logger
    config?: ProcessorConfig
    protected PROCESS_PROMPT = `现在你是一个翻译，接下来会给你日语或英语，请翻译以下日语或英语为简体中文，只输出译文，不要输出原文。如果是带有# hash tag的标签，不需要翻译。如果无法翻译请输出："${PROCESSOR_ERROR_FALLBACK}"`

    constructor(api_key: string, log?: Logger, config?: ProcessorConfig) {
        super()
        this.api_key = api_key
        this.log = log
        this.config = config
    }

    async init(): Promise<void> {
        this.log = this.log?.child({ label: this.NAME, subservice: 'processor' })
        this.log?.info(`loaded with prompt ${this.config?.prompt || this.PROCESS_PROMPT}`)
        this.log?.debug(`loaded with config ${this.config}`)
    }

    async drop(..._args: any[]): Promise<void> {
        noop()
    }

    public abstract process(text: string): Promise<string>

    static isValidResult(text?: string | null): boolean {
        return Boolean(text) && text !== PROCESSOR_ERROR_FALLBACK
    }
}

export interface ProcessorPlugin {
    provider: ProcessorProvider
    create: (apiKey: string, log?: Logger, config?: ProcessorConfig) => BaseProcessor
}

class ProcessorRegistry {
    private static instance: ProcessorRegistry
    private plugins: Map<string, ProcessorPlugin> = new Map()

    private constructor() { }

    static getInstance(): ProcessorRegistry {
        if (!ProcessorRegistry.instance) {
            ProcessorRegistry.instance = new ProcessorRegistry()
        }
        return ProcessorRegistry.instance
    }

    register(plugin: ProcessorPlugin): this {
        const key = plugin.provider.toLowerCase()
        if (this.plugins.has(key)) {
            throw new Error(`Processor plugin ${plugin.provider} already registered`)
        }
        this.plugins.set(key, plugin)
        return this
    }

    find(provider: ProcessorProvider | string): ProcessorPlugin | null {
        return this.plugins.get(provider.toLowerCase()) || null
    }

    async create(
        provider: ProcessorProvider | string,
        apiKey: string,
        log?: Logger,
        config?: ProcessorConfig,
    ): Promise<BaseProcessor> {
        const plugin = this.find(provider)
        if (!plugin) {
            throw new Error(`Unknown processor provider: ${provider}`)
        }

        const processor = plugin.create(apiKey, log, config)
        await processor.init()
        return processor
    }

    getRegisteredProviders(): string[] {
        return Array.from(this.plugins.keys())
    }
}

export { BaseProcessor, PROCESSOR_ERROR_FALLBACK, ProcessorRegistry }
