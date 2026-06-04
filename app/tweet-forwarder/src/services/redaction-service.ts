const SENSITIVE_KEY_PATTERN = /(secret|token|password|passwd|cookie|sessdata|bili_jct|csrf|auth|waf_bypass)/i

function redactSecrets<T>(value: T): T {
    if (Array.isArray(value)) {
        return value.map((item) => redactSecrets(item)) as T
    }
    if (!value || typeof value !== 'object') {
        return value
    }
    const output: Record<string, unknown> = {}
    for (const [key, entry] of Object.entries(value as Record<string, unknown>)) {
        if (SENSITIVE_KEY_PATTERN.test(key)) {
            output[key] = entry ? '[redacted]' : entry
            continue
        }
        output[key] = redactSecrets(entry)
    }
    return output as T
}

export { redactSecrets }
