import crypto from 'crypto'

/**
 * Bilibili app-protocol request signing (posting route B: `dynamic_api: 'app'`).
 *
 * Ported from the RE case's protocol kit (`scratch/bili-bbshow-re/protocol-kit/code/sign.py`,
 * spec `docs/01-auth-signature.md`), kept dependency-free (node:crypto only):
 *
 *   1. business params + common app params + `ts` (auto-injected when absent)
 *   2. canonical query: keys sorted ascending (TreeMap / String natural order),
 *      each key and value percent-encoded per RFC3986 (unreserved = A-Za-z0-9-_.~,
 *      everything else as %XX of its UTF-8 bytes, hex UPPERCASE), joined with `&`
 *   3. sign = MD5(canonical query + appsec) as lowercase hex  (candidate c1;
 *      the concatenation order and hex case are DERIVED/PENDING in the case file)
 *   4. final query = canonical query + '&sign=' + sign
 *
 * ⚠ appkey<->appsec pairing is UNADJUDICATED in the case file (GAP B2): two mutually
 * exclusive readings (A = ELF `.data.rel.ro` adjacent slots, B = SA-F doc reading)
 * cover the same 8+8 key/sec sets with different partners. This module records ONE
 * explicit choice and never silently switches:
 *
 *   DEFAULT_APPKEY = 1d8b6e7d45233436 (android)
 *   DEFAULT_APPSEC = 36efcfed79309338ced0380abd824ac1
 *     = kit pairing B for this key = the external public `android` pairing
 *       (corroborated by years of public third-party tooling; also the pairing the
 *        experiment accepted live on 2026-10-02).
 *
 * If a request is ever rejected at the sign/appkey gate, try the documented
 * alternates in BILI_APPKEY_APPSEC_PAIRINGS explicitly — do not guess.
 */

const UNRESERVED = /^[A-Za-z0-9\-_.~]$/

/** kofua.ds.h() / SignedQuery.urlEncode equivalent: RFC3986 percent-encoding. */
function pctEncode(input: string): string {
    let out = ''
    for (const ch of input) {
        if (UNRESERVED.test(ch)) {
            out += ch
        } else {
            for (const byte of Buffer.from(ch, 'utf8')) {
                out += '%' + byte.toString(16).toUpperCase().padStart(2, '0')
            }
        }
    }
    return out
}

/**
 * Full 8+8 appkey/appsec tables from the case file (sign.py). Set A = ELF adjacent
 * slots, set B = SA-F doc reading + external pairs; the union is identical, only the
 * pairings differ. See docs/01-auth-signature.md §3.2.
 */
const BILI_APPKEY_APPSEC_PAIRINGS: { A: Record<string, string>; B: Record<string, string> } = {
    A: {
        '783bbb7264451d82': '560c52ccd288fed045859ed18bffd973',
        '07da50c9a0bf829f': '2653583c8873dea268ab9386918b1d65',
        '191c3b6b975af184': '25bdede4e1581c836cab73a48790ca6e',
        'bb3101000e232e27': '1673b15a09ef5e4427627f47b03a0578',
        'ae57252b0c09105d': '36efcfed79309338ced0380abd824ac1',
        'dfca71928277209b': 'c75875c596a69eb55bd119e74b07cfe3',
        '7d089525d3611b1c': 'b5475a8825547a4fc26c7d518eaaa02e',
        '1d8b6e7d45233436': 'acd495b248ec528c2eed1e862d393126',
    },
    B: {
        '07da50c9a0bf829f': 'c75875c596a69eb55bd119e74b07cfe3',
        '1d8b6e7d45233436': '36efcfed79309338ced0380abd824ac1',
    },
}

const DEFAULT_APPKEY = '1d8b6e7d45233436'
const DEFAULT_APPSEC = '36efcfed79309338ced0380abd824ac1'

/**
 * Common app parameters injected by the client's DefaultRequestInterceptor
 * (docs/01 §4). `appkey`/`ts`/`sign` are added at signing time.
 */
const BILI_APP_COMMON_PARAMS: Record<string, string> = {
    build: '8990400',
    mobi_app: 'android',
    platform: 'android',
    channel: 'bili',
    app_ver: '8.99.0',
}

const BILI_APP_USER_AGENT = 'Mozilla/5.0 BiliDroid/8.99.0 (bbcallen@gmail.com)'

type BiliAppSignOptions = {
    /** 16-hex appkey; defaults to the recorded android key above. */
    appkey?: string
    /** 32-hex appsec; defaults to DEFAULT_APPSEC (the recorded explicit pairing). */
    appsec?: string
    /** Explicit pairing alternative (A|B) instead of an explicit appsec. */
    pairing?: 'A' | 'B'
    /** Override the timestamp (tests; production injects the current unix time). */
    ts?: string
}

function resolveAppsec(appkey: string, opts: BiliAppSignOptions = {}): string {
    if (opts.appsec) {
        return opts.appsec
    }
    if (opts.pairing) {
        const sec = BILI_APPKEY_APPSEC_PAIRINGS[opts.pairing][appkey]
        if (!sec) {
            throw new Error(
                `Bilibili appkey ${appkey} is not documented under pairing ${opts.pairing}; ` +
                    `pass an explicit appsec instead of guessing (GAP B2 pairings are unadjudicated)`,
            )
        }
        return sec
    }
    return DEFAULT_APPSEC
}

/**
 * Sign exactly the given canonical parameters (no common-param merging): keys sorted
 * ascending, RFC3986 percent-encoding, sign = MD5(query + appsec) lowercase hex,
 * final = query + '&sign=' + sign. This is the core port of sign.py and is pinned to the
 * RE case's worked example by tests.
 */
function signCanonicalQuery(params: Record<string, string>, appsec: string): string {
    const query = Object.keys(params)
        .sort()
        .map((key) => `${pctEncode(key)}=${pctEncode(params[key] ?? '')}`)
        .join('&')
    const sign = crypto.createHash('md5').update(query + appsec, 'utf8').digest('hex')
    return `${query}&sign=${sign}`
}

/**
 * Build the signed canonical query (everything after `?` on the wire). `params`
 * carries the business fields that ride in the query; form fields are NOT part of
 * the signature (they are signed on the @Query side only, per docs/02 convention).
 */
function signAppQuery(params: Record<string, string>, opts: BiliAppSignOptions = {}): string {
    const appkey = opts.appkey || DEFAULT_APPKEY
    const appsec = resolveAppsec(appkey, opts)
    const merged: Record<string, string> = { ...BILI_APP_COMMON_PARAMS, ...params, appkey }
    if (opts.ts !== undefined) {
        merged.ts = opts.ts
    } else if (merged.ts === undefined) {
        merged.ts = String(Math.floor(Date.now() / 1000))
    }
    return signCanonicalQuery(merged, appsec)
}

export {
    BILI_APP_COMMON_PARAMS,
    BILI_APP_USER_AGENT,
    BILI_APPKEY_APPSEC_PAIRINGS,
    DEFAULT_APPKEY,
    DEFAULT_APPSEC,
    pctEncode,
    resolveAppsec,
    signAppQuery,
    signCanonicalQuery,
    type BiliAppSignOptions,
}
