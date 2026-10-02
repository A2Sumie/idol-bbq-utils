import { expect, test } from 'bun:test'
import {
    BILI_APPKEY_APPSEC_PAIRINGS,
    pctEncode,
    resolveAppsec,
    signAppQuery,
    signCanonicalQuery,
} from './bilibili-app-sign'

// Worked example from the RE case (docs/01-auth-signature.md §2.5, protocol-kit sign.py):
// fixed ts so the vector is reproducible; both unadjudicated pairings are pinned here.
const EXAMPLE_PARAMS: Record<string, string> = {
    appkey: '07da50c9a0bf829f',
    build: '8990400',
    channel: 'bili',
    mobi_app: 'android_i',
    msg: 'a b/c~d',
    platform: 'android',
    ts: '1780000000',
    vmid: '2',
}
const EXAMPLE_QUERY =
    'appkey=07da50c9a0bf829f&build=8990400&channel=bili&mobi_app=android_i' +
    '&msg=a%20b%2Fc~d&platform=android&ts=1780000000&vmid=2'

test('signCanonicalQuery reproduces the protocol-kit worked example (pairing A)', () => {
    const appsec = BILI_APPKEY_APPSEC_PAIRINGS.A['07da50c9a0bf829f']!
    const signed = signCanonicalQuery(EXAMPLE_PARAMS, appsec)
    expect(signed).toBe(`${EXAMPLE_QUERY}&sign=a66508fd5e395c430a54333754c8b8c1`)
})

test('signCanonicalQuery reproduces the protocol-kit worked example (pairing B)', () => {
    const appsec = BILI_APPKEY_APPSEC_PAIRINGS.B['07da50c9a0bf829f']!
    const signed = signCanonicalQuery(EXAMPLE_PARAMS, appsec)
    expect(signed).toBe(`${EXAMPLE_QUERY}&sign=4a6cd8c787647c8f5e489d56197b334c`)
})

test('pctEncode follows RFC3986: unreserved stays, others become uppercase %XX of UTF-8 bytes', () => {
    expect(pctEncode('a b/c~d')).toBe('a%20b%2Fc~d')
    expect(pctEncode('A-Za-z0-9-_.~')).toBe('A-Za-z0-9-_.~')
    expect(pctEncode('动态')).toBe('%E5%8A%A8%E6%80%81')
    expect(pctEncode('【】')).toBe('%E3%80%90%E3%80%91')
})

test('resolveAppsec records one explicit pairing and never guesses', () => {
    // Default is the recorded explicit choice (kit pairing B for the android key = external pair).
    expect(resolveAppsec('1d8b6e7d45233436')).toBe('36efcfed79309338ced0380abd824ac1')
    // Explicit pairing lookup works per set...
    expect(resolveAppsec('07da50c9a0bf829f', { pairing: 'A' })).toBe('2653583c8873dea268ab9386918b1d65')
    // ...and a key outside the chosen set hard-fails instead of borrowing the other set's value.
    expect(() => resolveAppsec('dfca71928277209b', { pairing: 'B' })).toThrow()
})

test('signAppQuery injects the recorded appkey/common params and a verifiable sign', () => {
    const signed = signAppQuery({ rid: '0', type: '4' }, { ts: '1780000000' })
    expect(signed).toContain('appkey=1d8b6e7d45233436')
    expect(signed).toContain('mobi_app=android')
    expect(signed).toContain('build=8990400')
    expect(signed).toContain('ts=1780000000')
    expect(signed).toMatch(/&sign=[0-9a-f]{32}$/)
    // Sign input is exactly the canonical query before &sign=.
    const query = signed.slice(0, signed.lastIndexOf('&sign='))
    const expectedSign = new Bun.CryptoHasher('md5').update(query + '36efcfed79309338ced0380abd824ac1').digest('hex')
    expect(signed.endsWith(`&sign=${expectedSign}`)).toBe(true)
})
