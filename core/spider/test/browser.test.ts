import { describe, expect, test } from 'bun:test'
import { resolveBrowserProfile } from '../src/utils/browser'

describe('resolveBrowserProfile', () => {
    test('keeps the selected mobile device profile metadata', () => {
        const profile = resolveBrowserProfile('mobile_ios_safari_portrait')

        expect(profile.deviceProfile).toBe('mobile_ios_safari_portrait')
        expect(profile.platform).toBe('iPhone')
        expect(profile.vendor).toBe('Apple Computer, Inc.')
        expect(profile.userAgentData).toBeNull()
        expect(profile.plugins).toHaveLength(0)
    })

    test('uses chrome-like desktop defaults', () => {
        const profile = resolveBrowserProfile('desktop_chrome')

        expect(profile.deviceProfile).toBe('desktop_chrome')
        expect(profile.platform).toBe('Win32')
        expect(profile.userAgent).toContain('Chrome/142.')
        expect(profile.extraHeaders?.['sec-ch-ua-platform']).toBe('"Windows"')
        expect(profile.plugins.length).toBeGreaterThan(0)
    })
})
