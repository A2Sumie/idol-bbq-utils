import type { Page, Viewport } from 'puppeteer-core'
import { UserAgent } from './http'

type BrowserMode = 'headless' | 'headed-xvfb'
type DeviceProfile = 'desktop_chrome' | 'mobile_ios_safari_portrait'

interface ProfileViewport extends Viewport {
    width: number
    height: number
}

interface BrowserProfileConfig {
    userAgent: string
    viewport: ProfileViewport
    extraHeaders?: Record<string, string>
    emulateTouch?: boolean
    locale?: string
    timezone?: string
}

const DEVICE_PROFILE_PRESETS: Record<DeviceProfile, BrowserProfileConfig> = {
    desktop_chrome: {
        userAgent: UserAgent.CHROME,
        viewport: {
            width: 1440,
            height: 900,
            deviceScaleFactor: 1,
            hasTouch: false,
            isLandscape: true,
            isMobile: false,
        },
        extraHeaders: {
            'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        },
        locale: 'ja-JP',
        timezone: 'Asia/Tokyo',
    },
    mobile_ios_safari_portrait: {
        userAgent: UserAgent.MOBILE_IOS_SAFARI,
        viewport: {
            width: 430,
            height: 932,
            deviceScaleFactor: 3,
            hasTouch: true,
            isLandscape: false,
            isMobile: true,
        },
        extraHeaders: {
            'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        },
        emulateTouch: true,
        locale: 'ja-JP',
        timezone: 'Asia/Tokyo',
    },
}

interface BrowserProfileOverrides {
    userAgent?: string
    viewport?: Partial<ProfileViewport>
    extraHeaders?: Record<string, string>
    locale?: string
    timezone?: string
}

function resolveBrowserProfile(
    deviceProfile: DeviceProfile = 'desktop_chrome',
    overrides: BrowserProfileOverrides = {},
): BrowserProfileConfig {
    const preset = DEVICE_PROFILE_PRESETS[deviceProfile]
    return {
        ...preset,
        ...overrides,
        viewport: {
            ...preset.viewport,
            ...(overrides.viewport || {}),
        },
        extraHeaders: {
            ...(preset.extraHeaders || {}),
            ...(overrides.extraHeaders || {}),
        },
        userAgent: overrides.userAgent || preset.userAgent,
    }
}

async function applyBrowserProfile(
    page: Page,
    deviceProfile: DeviceProfile = 'desktop_chrome',
    overrides: BrowserProfileOverrides = {},
) {
    const profile = resolveBrowserProfile(deviceProfile, overrides)

    await page.setUserAgent(profile.userAgent)
    await page.setViewport(profile.viewport)
    if (profile.extraHeaders && Object.keys(profile.extraHeaders).length > 0) {
        await page.setExtraHTTPHeaders(profile.extraHeaders)
    }
    if (profile.timezone) {
        await page.emulateTimezone(profile.timezone).catch(() => null)
    }
    await page.setBypassCSP(true).catch(() => null)

    await page.evaluateOnNewDocument(
        ({ locale, emulateTouch, deviceProfile }) => {
            try {
                const isMobile = deviceProfile === 'mobile_ios_safari_portrait'
                Object.defineProperty(navigator, 'language', {
                    configurable: true,
                    get: () => locale,
                })
                Object.defineProperty(navigator, 'languages', {
                    configurable: true,
                    get: () => [locale, 'ja', 'en-US'],
                })
                Object.defineProperty(navigator, 'maxTouchPoints', {
                    configurable: true,
                    get: () => (emulateTouch ? 5 : 0),
                })
                Object.defineProperty(navigator, 'webdriver', {
                    configurable: true,
                    get: () => false,
                })
                Object.defineProperty(navigator, 'platform', {
                    configurable: true,
                    get: () => (isMobile ? 'iPhone' : 'MacIntel'),
                })
                if (isMobile) {
                    Object.defineProperty(navigator, 'standalone', {
                        configurable: true,
                        get: () => false,
                    })
                    Object.defineProperty(window, 'orientation', {
                        configurable: true,
                        get: () => 0,
                    })
                    if (!('ontouchstart' in window)) {
                        Object.defineProperty(window, 'ontouchstart', {
                            configurable: true,
                            get: () => null,
                        })
                    }
                    const orientation = {
                        type: 'portrait-primary',
                        angle: 0,
                        onchange: null,
                        addEventListener: () => undefined,
                        removeEventListener: () => undefined,
                        dispatchEvent: () => false,
                    }
                    Object.defineProperty(screen, 'orientation', {
                        configurable: true,
                        get: () => orientation,
                    })
                    const uaData = {
                        brands: [
                            {
                                brand: 'Safari',
                                version: '18',
                            },
                        ],
                        mobile: true,
                        platform: 'iOS',
                        getHighEntropyValues: async (hints: Array<string>) => {
                            const values: Record<string, unknown> = {
                                mobile: true,
                                platform: 'iOS',
                                platformVersion: '18.0.0',
                                architecture: 'arm',
                                model: 'iPhone',
                            }
                            return hints.reduce(
                                (acc, hint) => {
                                    if (hint in values) {
                                        acc[hint] = values[hint]
                                    }
                                    return acc
                                },
                                {} as Record<string, unknown>,
                            )
                        },
                    }
                    Object.defineProperty(navigator, 'userAgentData', {
                        configurable: true,
                        get: () => uaData,
                    })
                }
            } catch {
                // Ignore profile shim failures.
            }
        },
        {
            locale: profile.locale || 'ja-JP',
            emulateTouch: Boolean(profile.emulateTouch),
            deviceProfile,
        },
    )
}

export { DEVICE_PROFILE_PRESETS, applyBrowserProfile, resolveBrowserProfile }
export type { BrowserMode, BrowserProfileConfig, BrowserProfileOverrides, DeviceProfile, ProfileViewport }
