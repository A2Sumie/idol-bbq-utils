/**
 * Inbound event idempotency + reaction rate limiting.
 *
 * Webhook deliveries (SnowLuma `httpClients` / NapCat httpPostEndpoints) are
 * at-least-once: a timeout or a restart makes the bridge re-POST the same
 * event. Reacting twice (double reply / double alert / double group action) is
 * worse than reacting zero times, so every event is keyed by
 * `NormalizedOneBotEvent.dedupeKey` (message_id when present) and recorded in a
 * bounded TTL cache before any handler runs.
 *
 * The cache is in-memory on purpose: no schema migration for a default-off
 * feature, and the redelivery window of the bridge is short. A restart may
 * therefore replay at most one TTL window of events — acceptable for reactions,
 * and documented in docs/snowluma.md.
 *
 * The same module keeps the per-scope handler limiter (cooldown + hourly
 * quota) that stops reaction storms: one noisy rule cannot flood a group, and a
 * failing handler cannot retry-storm the bridge because every reaction is a
 * single-shot call classified by onebot-semantics.
 */

export interface DedupeCacheOptions {
    ttlMs: number
    maxEntries: number
    now?: () => number
}

export class EventDedupeCache {
    private readonly ttlMs: number
    private readonly maxEntries: number
    private readonly now: () => number
    private readonly entries = new Map<string, number>()

    constructor(options: DedupeCacheOptions) {
        this.ttlMs = Math.max(1000, Number(options.ttlMs) || 0)
        this.maxEntries = Math.max(16, Number(options.maxEntries) || 0)
        this.now = options.now || (() => Date.now())
    }

    private sweep() {
        const cutoff = this.now() - this.ttlMs
        for (const [key, seenAt] of this.entries) {
            if (seenAt >= cutoff && this.entries.size <= this.maxEntries) break
            if (seenAt < cutoff) this.entries.delete(key)
        }
        while (this.entries.size > this.maxEntries) {
            const oldest = this.entries.keys().next()
            if (oldest.done) break
            this.entries.delete(oldest.value)
        }
    }

    /** True when this key was already recorded inside the TTL window. */
    isDuplicate(key: string): boolean {
        this.sweep()
        const seenAt = this.entries.get(key)
        return seenAt !== undefined && seenAt >= this.now() - this.ttlMs
    }

    /** Record a key; returns false when it was already present (duplicate). */
    mark(key: string): boolean {
        this.sweep()
        const cutoff = this.now() - this.ttlMs
        const seenAt = this.entries.get(key)
        if (seenAt !== undefined && seenAt >= cutoff) return false
        this.entries.set(key, this.now())
        this.sweep()
        return true
    }

    size(): number {
        return this.entries.size
    }
}

export interface RateLimitRule {
    cooldownMs?: number
    maxPerWindow?: number
    windowMs?: number
}

export interface RateLimitDecision {
    allowed: boolean
    reason?: 'cooldown' | 'quota'
    retryAfterMs?: number
}

interface RateLimitState {
    lastAt: number
    windowStart: number
    count: number
}

export class HandlerRateLimiter {
    private readonly now: () => number
    private readonly states = new Map<string, RateLimitState>()

    constructor(now?: () => number) {
        this.now = now || (() => Date.now())
    }

    /**
     * Check (and on allow, consume) one slot for `key` — typically
     * `${handlerId}:${scope}` where scope is the group or private chat.
     */
    consume(key: string, rule: RateLimitRule = {}): RateLimitDecision {
        const now = this.now()
        const cooldownMs = Math.max(0, Number(rule.cooldownMs) || 0)
        const windowMs = Math.max(1000, Number(rule.windowMs) || 60 * 60 * 1000)
        const maxPerWindow = Number(rule.maxPerWindow)
        const state = this.states.get(key) || { lastAt: 0, windowStart: now, count: 0 }

        if (cooldownMs > 0 && state.lastAt > 0 && now - state.lastAt < cooldownMs) {
            this.states.set(key, state)
            return { allowed: false, reason: 'cooldown', retryAfterMs: cooldownMs - (now - state.lastAt) }
        }
        if (Number.isFinite(maxPerWindow) && maxPerWindow > 0) {
            if (now - state.windowStart >= windowMs) {
                state.windowStart = now
                state.count = 0
            }
            if (state.count >= maxPerWindow) {
                this.states.set(key, state)
                return { allowed: false, reason: 'quota', retryAfterMs: state.windowStart + windowMs - now }
            }
        }
        state.lastAt = now
        state.count += 1
        this.states.set(key, state)
        if (this.states.size > 2048) {
            // Bounded: drop the oldest half of the scopes when a long tail of
            // chats accumulates. Losing a limiter state only means one extra
            // reaction may slip through after a very long idle period.
            const keys = Array.from(this.states.keys()).slice(0, Math.floor(this.states.size / 2))
            for (const stale of keys) this.states.delete(stale)
        }
        return { allowed: true }
    }
}
