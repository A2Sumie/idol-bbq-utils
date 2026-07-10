import type { AppConfig, Processor, ProcessorConfig } from '@/types'
import { ProcessorProvider } from '@/types/processor'
import { getHy3CircuitBreaker } from '@/services/hy3-circuit-breaker-service'

const OPENCODE_GO_CHAT_COMPLETIONS_URL = 'https://opencode.ai/zen/go/v1/chat/completions'
const OPENCODE_ZEN_CHAT_COMPLETIONS_URL = 'https://opencode.ai/zen/v1/chat/completions'

interface ProcessorModelCapability {
    provider_id: string
    model_id: string
    display_name: string
    api: 'openai-completions'
    input: Array<'text'>
    reasoning: boolean
    context_window: number
    max_output_tokens: number
    cost_per_million?: {
        input: number
        output: number
        cache_read: number
        cache_write: number
    }
    estimated_go_requests?: {
        per_5h: number
    }
    compat?: {
        max_tokens_field: string
        thinking_format: string
        requires_reasoning_content_on_assistant_messages: boolean
    }
}

const OPENCODE_GO_MODEL_CAPABILITIES: Record<string, ProcessorModelCapability> = {
    'deepseek-v4-pro': {
        provider_id: 'opencode-go',
        model_id: 'deepseek-v4-pro',
        display_name: 'DeepSeek V4 Pro',
        api: 'openai-completions',
        input: ['text'],
        reasoning: true,
        context_window: 1_000_000,
        max_output_tokens: 384_000,
        cost_per_million: {
            input: 1.74,
            output: 3.48,
            cache_read: 0.0145,
            cache_write: 0,
        },
        estimated_go_requests: {
            per_5h: 3_450,
        },
        compat: {
            max_tokens_field: 'max_tokens',
            thinking_format: 'deepseek',
            requires_reasoning_content_on_assistant_messages: true,
        },
    },
    'deepseek-v4-flash': {
        provider_id: 'opencode-go',
        model_id: 'deepseek-v4-flash',
        display_name: 'DeepSeek V4 Flash',
        api: 'openai-completions',
        input: ['text'],
        reasoning: true,
        context_window: 1_000_000,
        max_output_tokens: 384_000,
        cost_per_million: {
            input: 0.14,
            output: 0.28,
            cache_read: 0.0028,
            cache_write: 0,
        },
        estimated_go_requests: {
            per_5h: 31_650,
        },
        compat: {
            max_tokens_field: 'max_tokens',
            thinking_format: 'deepseek',
            requires_reasoning_content_on_assistant_messages: true,
        },
    },
    'hy3-free': {
        provider_id: 'opencode-zen',
        model_id: 'hy3-free',
        display_name: 'HY3 Free (stealth)',
        api: 'openai-completions',
        input: ['text'],
        reasoning: false,
        context_window: 128_000,
        max_output_tokens: 16_384,
        cost_per_million: {
            input: 0,
            output: 0,
            cache_read: 0,
            cache_write: 0,
        },
        compat: {
            max_tokens_field: 'max_tokens',
            thinking_format: 'none',
            requires_reasoning_content_on_assistant_messages: false,
        },
    },
}

function normalizeModelId(modelId?: string | null) {
    return String(modelId || '')
        .trim()
        .toLowerCase()
}

function normalizeProvider(provider?: string | null) {
    return String(provider || '')
        .trim()
        .toLowerCase()
}

function resolveDefaultModelId(provider: string, config?: ProcessorConfig) {
    if (config?.model_id) {
        return config.model_id
    }
    const normalized = normalizeProvider(provider)
    if (
        normalized === normalizeProvider(ProcessorProvider.DeepSeekV4Pro) ||
        normalized === 'v4pro' ||
        normalized === 'dsv4pro' ||
        normalized === 'deepseekv4pro' ||
        normalized === 'deepseek-v4-pro' ||
        normalized === 'ds-v4pro'
    ) {
        return 'deepseek-v4-pro'
    }
    if (
        normalized === normalizeProvider(ProcessorProvider.DeepSeekV4Flash) ||
        normalized === 'v4flash' ||
        normalized === 'dsv4flash' ||
        normalized === 'deepseekv4flash' ||
        normalized === 'deepseek-v4-flash'
    ) {
        return 'deepseek-v4-flash'
    }
    if (
        normalized === normalizeProvider(ProcessorProvider.Hy3Free) ||
        normalized === 'hy3-free' ||
        normalized === 'hy3' ||
        normalized === 'hy3free'
    ) {
        return 'hy3-free'
    }
    return config?.model_id || provider
}

function endpointHost(baseUrl?: string | null) {
    const value = String(baseUrl || OPENCODE_GO_CHAT_COMPLETIONS_URL).trim()
    try {
        return new URL(value).host
    } catch {
        return 'invalid-url'
    }
}

function requestDefaults(config?: ProcessorConfig) {
    return {
        temperature: typeof config?.temperature === 'number' ? config.temperature : undefined,
        max_tokens: typeof config?.max_tokens === 'number' ? config.max_tokens : undefined,
        response_format: config?.response_format,
        request_timeout_ms: config?.request_timeout_ms,
        thinking: config?.extended_payload?.thinking,
    }
}

function buildProcessorModelCapability(processor: Processor) {
    const cfg = processor.cfg_processor
    const modelId = resolveDefaultModelId(processor.provider, cfg)
    const normalizedModelId = normalizeModelId(modelId)
    const known = OPENCODE_GO_MODEL_CAPABILITIES[normalizedModelId]
    const isHy3 = normalizedModelId === 'hy3-free'

    return {
        processor_id: processor.id,
        processor_name: processor.name,
        provider: processor.provider,
        model_id: modelId,
        configured_name: cfg?.name,
        endpoint_host: endpointHost(cfg?.base_url),
        request_defaults: requestDefaults(cfg),
        capability: known || null,
        ...(isHy3
            ? {
                  hy3: {
                      frozen: getHy3CircuitBreaker().isFrozen(),
                      breaker: getHy3CircuitBreaker().getDetailedStatus(),
                  },
              }
            : {}),
    }
}

function buildProcessorModelCapabilities(config: AppConfig) {
    return (config.processors || []).map(buildProcessorModelCapability)
}

function getKnownModelCapability(modelId?: string | null) {
    return OPENCODE_GO_MODEL_CAPABILITIES[normalizeModelId(modelId)] || null
}

export {
    OPENCODE_GO_CHAT_COMPLETIONS_URL,
    OPENCODE_ZEN_CHAT_COMPLETIONS_URL,
    buildProcessorModelCapabilities,
    getKnownModelCapability,
    resolveDefaultModelId,
}
export type { ProcessorModelCapability }
