export const DEFAULT_CRON_SECOND = 45

export function normalizeCronSecond(cron: string | undefined | null, second = DEFAULT_CRON_SECOND) {
    const value = String(cron || '').trim()
    if (!value) {
        return value
    }

    const parts = value.split(/\s+/)
    if (parts.length === 5) {
        return `${second} ${value}`
    }
    return value
}
