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

function collectSensitiveConfigPaths(value: unknown): Array<string> {
    const paths = new Set<string>()

    function visit(entry: unknown, basePath: string) {
        if (Array.isArray(entry)) {
            entry.forEach((item, index) => visit(item, `${basePath}[${index}]`))
            return
        }
        if (!entry || typeof entry !== 'object') {
            return
        }

        for (const [key, child] of Object.entries(entry as Record<string, unknown>)) {
            const childPath = basePath ? `${basePath}.${key}` : key
            if (SENSITIVE_KEY_PATTERN.test(key)) {
                if (child) {
                    paths.add(childPath)
                }
                continue
            }
            visit(child, childPath)
        }
    }

    visit(value, '')
    return Array.from(paths).sort()
}

export { collectSensitiveConfigPaths, redactSecrets, SENSITIVE_KEY_PATTERN }
