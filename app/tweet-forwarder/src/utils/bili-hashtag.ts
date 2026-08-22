/**
 * X (Twitter) hashtag -> Bilibili hashtag conversion.
 *
 * Bilibili topics use the paired form `#content#`; X hashtags are single-leading
 * (`#tag`, terminated by the first invalid character). This module rewrites X-style
 * hashtags found in a text into the Bilibili paired form, following the twitter-text
 * autolink spec (VALID_HASHTAG):
 *
 * - Marker: `#` or full-width `＃`. The character before the marker must be the start
 *   of the text/line or a non-word character — `text#tag` is NOT a hashtag
 *   (boundary excludes Unicode letters, marks, decimal digits, `_` and `&`).
 * - Tag characters: `\p{L}` + `\p{M}` + `\p{Nd}` + `_` + the twitter-text specials
 *   (Hebrew maqaf/geresh/gershayim, katakana middle dot `・`, prolonged sound mark
 *   `ー`, ZWNJ/ZWJ). ASCII `-` is NOT valid, so `#COVID-19` parses as `#COVID` only.
 * - The tag must contain at least one letter (`\p{L}`/`\p{M}`); pure numeric tags
 *   like `#1234` are not hashtags on X and are left untouched.
 * - Idempotency: a tag immediately followed by `#`/`＃` is treated as already paired
 *   (`#tag#`) and left as-is.
 * - Defensive: tag characters never include whitespace, so a newline can never end
 *   up inside the emitted `#...#`.
 */

// \u30fc (ー) is \p{Lm} and already covered by \p{L}; kept explicit for readability.
const HASHTAG_SPECIALS = '־׳״・ー‌‍'
const HASHTAG_CHAR_CLASS = `[\\p{L}\\p{M}\\p{Nd}_${HASHTAG_SPECIALS}]`
const HASHTAG_BOUNDARY_CLASS = `[^\\p{L}\\p{M}\\p{Nd}_&]`

const X_HASHTAG_REGEX = new RegExp(
    `(^|${HASHTAG_BOUNDARY_CLASS})[#＃](${HASHTAG_CHAR_CLASS}+)`,
    'gu',
)
const X_HASHTAG_LETTER_REGEX = /[\p{L}\p{M}]/u
const CLOSING_MARKERS = new Set(['#', '＃'])

function convertXHashtagsToBiliFormat(text: string): string {
    if (!text || (!text.includes('#') && !text.includes('＃'))) {
        return text
    }
    return text.replace(X_HASHTAG_REGEX, (match, boundary: string, tag: string, offset: number, whole: string) => {
        // Pure numeric / underscore tags are not X hashtags.
        if (!X_HASHTAG_LETTER_REGEX.test(tag)) {
            return match
        }
        // Already in paired `#tag#` form: leave untouched (idempotency).
        if (CLOSING_MARKERS.has(whole[offset + match.length] || '')) {
            return match
        }
        return `${boundary}#${tag}#`
    })
}

export { convertXHashtagsToBiliFormat }
