/**
 * Default timeout for a single upstream HTTP fetch. Keeping it bounded prevents a hung
 * request from silently blocking a crawler slot and from defeating retry/cooldown logic.
 */
const DEFAULT_FETCH_TIMEOUT_MS = 20000

/**
 * Error raised when an upstream HTTP fetch returns a non-2xx status. Carries the status so
 * callers (and crawl-error classification) can distinguish auth / rate-limit / transient cases.
 */
class HttpStatusError extends Error {
    readonly status: number
    readonly url: string
    constructor(status: number, url: string) {
        super(`HTTP ${status} for ${url}`)
        this.name = 'HttpStatusError'
        this.status = status
        this.url = url
    }
}

/**
 * Error raised when an upstream HTTP fetch exceeds the timeout budget.
 */
class HttpTimeoutError extends Error {
    readonly url: string
    readonly timeoutMs: number
    constructor(url: string, timeoutMs: number) {
        super(`HTTP request timed out after ${timeoutMs}ms for ${url}`)
        this.name = 'HttpTimeoutError'
        this.url = url
        this.timeoutMs = timeoutMs
    }
}

interface DownloadWebpageOptions {
    /** Per-request timeout in milliseconds. Defaults to DEFAULT_FETCH_TIMEOUT_MS. */
    timeout?: number
    /** When true (default), a non-2xx response throws HttpStatusError instead of returning it. */
    throwOnError?: boolean
}

namespace HTTPClient {
    export async function download_webpage(
        url: string,
        headers: Record<string, string> = {},
        options: DownloadWebpageOptions = {},
    ): Promise<Response> {
        const timeout = Math.max(1, options.timeout ?? DEFAULT_FETCH_TIMEOUT_MS)
        const throwOnError = options.throwOnError ?? true
        const controller = new AbortController()
        const timeoutId = setTimeout(() => controller.abort(), timeout)
        let response: Response
        try {
            response = await fetch(url, {
                method: 'GET',
                headers: {
                    'user-agent': UserAgent.CHROME,
                    ...headers,
                },
                signal: controller.signal,
            })
        } catch (error) {
            if (controller.signal.aborted) {
                throw new HttpTimeoutError(url, timeout)
            }
            throw error
        } finally {
            clearTimeout(timeoutId)
        }
        if (throwOnError && response.status >= 400) {
            throw new HttpStatusError(response.status, url)
        }
        return response
    }
}

const UserAgent = {
    CHROME: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36',
    FIREFOX: 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:136.0) Gecko/20100101 Firefox/136.0',
    SAFARI: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0.3 Safari/605.1.15',
    MOBILE_IOS_SAFARI:
        'Mozilla/5.0 (iPhone; CPU iPhone OS 18_3 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.3 Mobile/15E148 Safari/604.1',
    MOBILE_ANDROID_CHROME:
        'Mozilla/5.0 (Linux; Android 14; SM-S918B) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Mobile Safari/537.36',
}

export { HTTPClient, UserAgent, HttpStatusError, HttpTimeoutError, DEFAULT_FETCH_TIMEOUT_MS }
export type { DownloadWebpageOptions }
