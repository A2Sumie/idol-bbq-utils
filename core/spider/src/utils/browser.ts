import type { Page, Viewport } from 'puppeteer-core'
import { UserAgent } from './http'

type BrowserMode = 'headless' | 'headed-xvfb'
type DeviceProfile = 'desktop_chrome' | 'mobile_ios_safari_portrait'
type ScreenOrientationType = 'landscape-primary' | 'portrait-primary'

interface ProfileViewport extends Viewport {
    width: number
    height: number
}

interface ProfileWindowSize {
    width: number
    height: number
}

interface ProfileScreen {
    width: number
    height: number
    availWidth: number
    availHeight: number
    colorDepth: number
    pixelDepth: number
    orientation: ScreenOrientationType
    angle: number
}

interface ProfileConnection {
    downlink: number
    effectiveType: string
    rtt: number
    saveData: boolean
}

interface ProfileMimeType {
    type: string
    suffixes: string
    description: string
}

interface ProfilePlugin {
    name: string
    filename: string
    description: string
    mimeTypes: Array<ProfileMimeType>
}

interface ProfileUserAgentBrandVersion {
    brand: string
    version: string
}

interface ProfileUserAgentData {
    brands: Array<ProfileUserAgentBrandVersion>
    fullVersionList?: Array<ProfileUserAgentBrandVersion>
    mobile: boolean
    platform: string
    platformVersion?: string
    architecture?: string
    bitness?: string
    model?: string
    wow64?: boolean
    fullVersion?: string
}

interface BrowserProfileConfig {
    deviceProfile: DeviceProfile
    userAgent: string
    viewport: ProfileViewport
    windowSize: ProfileWindowSize
    screen: ProfileScreen
    extraHeaders?: Record<string, string>
    emulateTouch?: boolean
    locale?: string
    timezone?: string
    platform: string
    vendor: string
    maxTouchPoints: number
    hardwareConcurrency: number
    deviceMemory?: number
    connection?: ProfileConnection
    plugins: Array<ProfilePlugin>
    pdfViewerEnabled?: boolean
    userAgentData?: ProfileUserAgentData | null
}

const CHROME_MAJOR_VERSION = '142'
const CHROME_FULL_VERSION = '142.0.7444.175'
const CHROME_BRANDS: Array<ProfileUserAgentBrandVersion> = [
    {
        brand: 'Not_A Brand',
        version: '99',
    },
    {
        brand: 'Chromium',
        version: CHROME_MAJOR_VERSION,
    },
    {
        brand: 'Google Chrome',
        version: CHROME_MAJOR_VERSION,
    },
]
const PDF_MIME_TYPES: Array<ProfileMimeType> = [
    {
        type: 'application/pdf',
        suffixes: 'pdf',
        description: 'Portable Document Format',
    },
    {
        type: 'text/pdf',
        suffixes: 'pdf',
        description: 'Portable Document Format',
    },
]
const CHROME_PDF_PLUGINS: Array<ProfilePlugin> = [
    {
        name: 'PDF Viewer',
        filename: 'internal-pdf-viewer',
        description: 'Portable Document Format',
        mimeTypes: PDF_MIME_TYPES,
    },
    {
        name: 'Chrome PDF Viewer',
        filename: 'internal-pdf-viewer',
        description: 'Portable Document Format',
        mimeTypes: PDF_MIME_TYPES,
    },
    {
        name: 'Chromium PDF Viewer',
        filename: 'internal-pdf-viewer',
        description: 'Portable Document Format',
        mimeTypes: PDF_MIME_TYPES,
    },
]

const DEVICE_PROFILE_PRESETS: Record<DeviceProfile, BrowserProfileConfig> = {
    desktop_chrome: {
        deviceProfile: 'desktop_chrome',
        userAgent: UserAgent.CHROME,
        viewport: {
            width: 1440,
            height: 900,
            deviceScaleFactor: 1,
            hasTouch: false,
            isLandscape: true,
            isMobile: false,
        },
        windowSize: {
            width: 1440,
            height: 980,
        },
        screen: {
            width: 1440,
            height: 980,
            availWidth: 1440,
            availHeight: 940,
            colorDepth: 24,
            pixelDepth: 24,
            orientation: 'landscape-primary',
            angle: 0,
        },
        extraHeaders: {
            'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
            'sec-ch-ua': `"Not_A Brand";v="99", "Chromium";v="${CHROME_MAJOR_VERSION}", "Google Chrome";v="${CHROME_MAJOR_VERSION}"`,
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        },
        locale: 'ja-JP',
        timezone: 'Asia/Tokyo',
        platform: 'Win32',
        vendor: 'Google Inc.',
        maxTouchPoints: 0,
        hardwareConcurrency: 8,
        deviceMemory: 8,
        connection: {
            downlink: 10,
            effectiveType: '4g',
            rtt: 100,
            saveData: false,
        },
        plugins: CHROME_PDF_PLUGINS,
        pdfViewerEnabled: true,
        userAgentData: {
            brands: CHROME_BRANDS,
            fullVersionList: CHROME_BRANDS.map((brand) => ({
                brand: brand.brand,
                version: brand.brand === 'Not_A Brand' ? '99.0.0.0' : CHROME_FULL_VERSION,
            })),
            mobile: false,
            platform: 'Windows',
            platformVersion: '10.0.0',
            architecture: 'x86',
            bitness: '64',
            model: '',
            wow64: false,
            fullVersion: CHROME_FULL_VERSION,
        },
    },
    mobile_ios_safari_portrait: {
        deviceProfile: 'mobile_ios_safari_portrait',
        userAgent: UserAgent.MOBILE_IOS_SAFARI,
        viewport: {
            width: 430,
            height: 932,
            deviceScaleFactor: 3,
            hasTouch: true,
            isLandscape: false,
            isMobile: true,
        },
        windowSize: {
            width: 430,
            height: 932,
        },
        screen: {
            width: 430,
            height: 932,
            availWidth: 430,
            availHeight: 932,
            colorDepth: 32,
            pixelDepth: 32,
            orientation: 'portrait-primary',
            angle: 0,
        },
        extraHeaders: {
            'accept-language': 'ja-JP,ja;q=0.9,en-US;q=0.8,en;q=0.7',
        },
        emulateTouch: true,
        locale: 'ja-JP',
        timezone: 'Asia/Tokyo',
        platform: 'iPhone',
        vendor: 'Apple Computer, Inc.',
        maxTouchPoints: 5,
        hardwareConcurrency: 6,
        plugins: [],
        userAgentData: null,
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
        (fingerprint) => {
            try {
                const isMobile = fingerprint.deviceProfile === 'mobile_ios_safari_portrait'
                const defineGetter = (target: object, property: string, value: unknown) => {
                    Object.defineProperty(target, property, {
                        configurable: true,
                        get: () => value,
                    })
                }
                const createNamedArray = (items: Array<Record<string, unknown>>, proto: object, namedKey: string) => {
                    const arrayLike = [] as Array<Record<string, unknown>>
                    Object.setPrototypeOf(arrayLike, proto)
                    Object.defineProperty(arrayLike, 'item', {
                        configurable: true,
                        value: (index: number) => arrayLike[index] || null,
                    })
                    Object.defineProperty(arrayLike, 'namedItem', {
                        configurable: true,
                        value: (name: string) =>
                            arrayLike.find((item) => item[namedKey] === name) || null,
                    })
                    for (const [index, item] of items.entries()) {
                        arrayLike[index] = item
                        const key = item[namedKey]
                        if (typeof key === 'string' && !(key in arrayLike)) {
                            Object.defineProperty(arrayLike, key, {
                                configurable: true,
                                enumerable: false,
                                value: item,
                            })
                        }
                    }
                    return arrayLike
                }
                const mimeTypeProto = typeof MimeType !== 'undefined' ? MimeType.prototype : Object.prototype
                const mimeTypeArrayProto =
                    typeof MimeTypeArray !== 'undefined' ? MimeTypeArray.prototype : Array.prototype
                const pluginProto = typeof Plugin !== 'undefined' ? Plugin.prototype : Object.prototype
                const pluginArrayProto =
                    typeof PluginArray !== 'undefined' ? PluginArray.prototype : Array.prototype
                const mimeTypeIndex = new Map<string, Record<string, unknown>>()
                for (const plugin of fingerprint.plugins as Array<Record<string, unknown>>) {
                    for (const mimeType of ((plugin.mimeTypes as Array<Record<string, unknown>> | undefined) || [])) {
                        const key = `${String(mimeType.type)}:${String(mimeType.suffixes)}`
                        if (!mimeTypeIndex.has(key)) {
                            const mimeTypeObject = {
                                type: mimeType.type,
                                suffixes: mimeType.suffixes,
                                description: mimeType.description,
                                enabledPlugin: null as Record<string, unknown> | null,
                            }
                            Object.setPrototypeOf(mimeTypeObject, mimeTypeProto)
                            mimeTypeIndex.set(key, mimeTypeObject)
                        }
                    }
                }
                const mimeTypes = Array.from(mimeTypeIndex.values())
                const mimeTypeArray = createNamedArray(mimeTypes, mimeTypeArrayProto, 'type')
                const plugins = fingerprint.plugins.map((plugin: Record<string, unknown>) => {
                    const pluginMimeTypes = ((plugin.mimeTypes as Array<Record<string, unknown>> | undefined) || []).map(
                        (mimeType) =>
                            mimeTypeIndex.get(`${String(mimeType.type)}:${String(mimeType.suffixes)}`) || null,
                    )
                    const pluginObject: Record<string, unknown> = {
                        name: plugin.name,
                        filename: plugin.filename,
                        description: plugin.description,
                        length: pluginMimeTypes.length,
                        item: (mimeTypeIndex: number) => pluginMimeTypes[mimeTypeIndex] || null,
                        namedItem: (name: string) =>
                            pluginMimeTypes.find((mimeType) => mimeType?.type === name) || null,
                    }
                    for (const [mimeTypeIndex, mimeType] of pluginMimeTypes.entries()) {
                        if (!mimeType) {
                            continue
                        }
                        pluginObject[mimeTypeIndex] = mimeType
                        if (typeof mimeType.type === 'string' && !(mimeType.type in pluginObject)) {
                            pluginObject[mimeType.type] = mimeType
                        }
                        mimeType.enabledPlugin = pluginObject
                    }
                    Object.setPrototypeOf(pluginObject, pluginProto)
                    return pluginObject
                })
                const pluginArray = createNamedArray(plugins, pluginArrayProto, 'name')

                defineGetter(navigator, 'language', fingerprint.locale)
                defineGetter(navigator, 'languages', [fingerprint.locale, 'ja', 'en-US'])
                defineGetter(navigator, 'maxTouchPoints', fingerprint.maxTouchPoints)
                defineGetter(navigator, 'webdriver', undefined)
                defineGetter(navigator, 'platform', fingerprint.platform)
                defineGetter(navigator, 'vendor', fingerprint.vendor)
                defineGetter(navigator, 'hardwareConcurrency', fingerprint.hardwareConcurrency)
                defineGetter(navigator, 'plugins', pluginArray)
                defineGetter(navigator, 'mimeTypes', mimeTypeArray)
                if (typeof fingerprint.pdfViewerEnabled === 'boolean') {
                    defineGetter(navigator, 'pdfViewerEnabled', fingerprint.pdfViewerEnabled)
                }
                if (typeof fingerprint.deviceMemory === 'number') {
                    defineGetter(navigator, 'deviceMemory', fingerprint.deviceMemory)
                }
                if (fingerprint.connection) {
                    const connection = {
                        downlink: fingerprint.connection.downlink,
                        effectiveType: fingerprint.connection.effectiveType,
                        onchange: null,
                        rtt: fingerprint.connection.rtt,
                        saveData: fingerprint.connection.saveData,
                        addEventListener: () => undefined,
                        removeEventListener: () => undefined,
                        dispatchEvent: () => false,
                    }
                    defineGetter(navigator, 'connection', connection)
                }
                if (fingerprint.userAgentData) {
                    const uaData = {
                        ...fingerprint.userAgentData,
                        getHighEntropyValues: async (hints: Array<string>) => {
                            const values: Record<string, unknown> = {
                                architecture: fingerprint.userAgentData?.architecture,
                                bitness: fingerprint.userAgentData?.bitness,
                                brands: fingerprint.userAgentData?.brands,
                                fullVersionList: fingerprint.userAgentData?.fullVersionList,
                                mobile: fingerprint.userAgentData?.mobile,
                                model: fingerprint.userAgentData?.model,
                                platform: fingerprint.userAgentData?.platform,
                                platformVersion: fingerprint.userAgentData?.platformVersion,
                                wow64: fingerprint.userAgentData?.wow64,
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
                        toJSON: () => ({
                            brands: fingerprint.userAgentData?.brands || [],
                            mobile: fingerprint.userAgentData?.mobile || false,
                            platform: fingerprint.userAgentData?.platform || '',
                        }),
                    }
                    defineGetter(navigator, 'userAgentData', uaData)
                } else {
                    defineGetter(navigator, 'userAgentData', undefined)
                }
                if (fingerprint.screen) {
                    defineGetter(screen, 'width', fingerprint.screen.width)
                    defineGetter(screen, 'height', fingerprint.screen.height)
                    defineGetter(screen, 'availWidth', fingerprint.screen.availWidth)
                    defineGetter(screen, 'availHeight', fingerprint.screen.availHeight)
                    defineGetter(screen, 'colorDepth', fingerprint.screen.colorDepth)
                    defineGetter(screen, 'pixelDepth', fingerprint.screen.pixelDepth)
                    defineGetter(screen, 'orientation', {
                        type: fingerprint.screen.orientation,
                        angle: fingerprint.screen.angle,
                        onchange: null,
                        addEventListener: () => undefined,
                        removeEventListener: () => undefined,
                        dispatchEvent: () => false,
                    })
                }
                if (fingerprint.windowSize) {
                    defineGetter(window, 'outerWidth', fingerprint.windowSize.width)
                    defineGetter(window, 'outerHeight', fingerprint.windowSize.height)
                }
                const originalQuery = navigator.permissions?.query?.bind(navigator.permissions)
                if (originalQuery) {
                    Object.defineProperty(navigator.permissions, 'query', {
                        configurable: true,
                        value: (parameters: { name?: string }) => {
                            if (parameters?.name === 'notifications') {
                                return Promise.resolve({
                                    state: Notification.permission,
                                    onchange: null,
                                    addEventListener: () => undefined,
                                    removeEventListener: () => undefined,
                                    dispatchEvent: () => false,
                                })
                            }
                            return originalQuery(parameters)
                        },
                    })
                }
                if (fingerprint.deviceProfile === 'desktop_chrome') {
                    const chromeObject = {
                        app: {
                            InstallState: {
                                DISABLED: 'disabled',
                                INSTALLED: 'installed',
                                NOT_INSTALLED: 'not_installed',
                            },
                            RunningState: {
                                CANNOT_RUN: 'cannot_run',
                                READY_TO_RUN: 'ready_to_run',
                                RUNNING: 'running',
                            },
                            isInstalled: false,
                        },
                        runtime: {},
                    }
                    defineGetter(window, 'chrome', chromeObject)
                } else {
                    try {
                        delete (window as Window & { chrome?: unknown }).chrome
                    } catch {
                        // Ignore delete failures.
                    }
                    defineGetter(window, 'chrome', undefined)
                }
                if (isMobile) {
                    defineGetter(navigator, 'standalone', false)
                    defineGetter(window, 'orientation', 0)
                    if (!('ontouchstart' in window)) {
                        defineGetter(window, 'ontouchstart', null)
                    }
                }
            } catch {
                // Ignore profile shim failures.
            }
        },
        {
            deviceProfile: profile.deviceProfile,
            platform: profile.platform,
            vendor: profile.vendor,
            maxTouchPoints: profile.maxTouchPoints,
            hardwareConcurrency: profile.hardwareConcurrency,
            deviceMemory: profile.deviceMemory,
            screen: profile.screen,
            windowSize: profile.windowSize,
            connection: profile.connection,
            plugins: profile.plugins,
            pdfViewerEnabled: profile.pdfViewerEnabled,
            userAgentData: profile.userAgentData,
            locale: profile.locale || 'ja-JP',
        },
    )
}

function buildBrowserRequestHeaders(
    deviceProfile: DeviceProfile = 'desktop_chrome',
    overrides: BrowserProfileOverrides = {},
) {
    const profile = resolveBrowserProfile(deviceProfile, overrides)
    return {
        'user-agent': profile.userAgent,
        ...(profile.extraHeaders || {}),
    }
}

export { DEVICE_PROFILE_PRESETS, applyBrowserProfile, buildBrowserRequestHeaders, resolveBrowserProfile }
export type {
    BrowserMode,
    BrowserProfileConfig,
    BrowserProfileOverrides,
    DeviceProfile,
    ProfileViewport,
}
