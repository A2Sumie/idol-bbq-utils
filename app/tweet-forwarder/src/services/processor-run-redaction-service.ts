import { redactSecrets } from './redaction-service'

type ProcessorRunEntryLike = Record<string, unknown> & {
    input?: unknown
    output?: unknown
    source_ref?: string | null
    error?: string | null
}

function parseObject(value: unknown): Record<string, unknown> | null {
    if (!value || typeof value !== 'object' || Array.isArray(value)) {
        return null
    }
    return value as Record<string, unknown>
}

function valueType(value: unknown) {
    if (value === null) return 'null'
    if (Array.isArray(value)) return 'array'
    return typeof value
}

function hasValue(value: unknown) {
    return value !== null && value !== undefined && value !== ''
}

function stringLength(value: unknown) {
    return typeof value === 'string' ? value.length : undefined
}

function publicScalar(value: unknown) {
    if (!hasValue(value)) {
        return null
    }
    if (['string', 'number', 'boolean'].includes(typeof value)) {
        return value
    }
    return null
}

function publicRequestSummary(request: unknown) {
    const value = parseObject(request)
    if (!value) {
        return {
            has_request: false,
            request_type: valueType(request),
        }
    }
    return {
        has_request: true,
        processor_id_present: hasValue(value.processorId),
        action: typeof value.action === 'string' ? value.action : null,
        platform: publicScalar(value.platform),
        row_id_present: hasValue(value.id),
        article_id_present: hasValue(value.a_id),
        user_id_present: hasValue(value.u_id),
        time_range_present: hasValue(value.start) || hasValue(value.end),
        text_present: hasValue(value.text),
        ...(stringLength(value.text) !== undefined ? { text_length: stringLength(value.text) } : {}),
        schedule_url_present: hasValue(value.scheduleUrl),
        schedule_api_key_present: hasValue(value.scheduleApiKey),
        schedule_user_agent_present: hasValue(value.scheduleUserAgent),
        schedule_waf_bypass_header_present: hasValue(value.scheduleWafBypassHeader),
        result_key_present: hasValue(value.resultKey),
    }
}

function publicProcessorInput(input: unknown) {
    const value = parseObject(input)
    if (!value) {
        return {
            redacted_input: true,
            input_type: valueType(input),
        }
    }
    return {
        redacted_input: true,
        request: publicRequestSummary(value.request),
        text_present: hasValue(value.text),
        ...(stringLength(value.text) !== undefined ? { text_length: stringLength(value.text) } : {}),
    }
}

function publicProcessorOutput(output: unknown) {
    const value = parseObject(output)
    if (!value) {
        return {
            redacted_output: true,
            output_type: valueType(output),
        }
    }
    return {
        redacted_output: true,
        raw_present: hasValue(value.raw),
        ...(stringLength(value.raw) !== undefined ? { raw_length: stringLength(value.raw) } : {}),
        parsed_present: hasValue(value.parsed),
        parsed_type: valueType(value.parsed),
        selected_present: hasValue(value.selected),
        selected_type: valueType(value.selected),
        result_key_present: hasValue(value.result_key),
        schedule_count: Array.isArray(value.schedules) ? value.schedules.length : 0,
    }
}

function redactProcessorRunForApi<T extends ProcessorRunEntryLike>(run: T): T {
    const redacted = redactSecrets(run) as T
    return {
        ...redacted,
        source_ref: run.source_ref ? '[redacted]' : run.source_ref,
        input: publicProcessorInput(run.input),
        output: publicProcessorOutput(run.output),
        error: run.error ? '[redacted]' : run.error,
    }
}

function redactProcessorRunsForApi<T extends ProcessorRunEntryLike>(runs: Array<T>): Array<T> {
    return runs.map((run) => redactProcessorRunForApi(run))
}

export { redactProcessorRunForApi, redactProcessorRunsForApi }
