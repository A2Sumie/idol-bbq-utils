import fs from 'fs'
import path from 'path'

type ResolveConfiguredCookieFilePathOptions = {
    configPath?: string
    cwd?: string
}

function resolveConfiguredCookieFilePath(
    cookieFile: string | null | undefined,
    options: ResolveConfiguredCookieFilePathOptions = {},
) {
    const value = String(cookieFile || '').trim()
    if (!value) {
        return null
    }

    const cwd = options.cwd || process.cwd()
    if (path.isAbsolute(value)) {
        if (fs.existsSync(value)) {
            return value
        }
        if (value.startsWith('/app/')) {
            return path.resolve(cwd, value.slice('/app/'.length))
        }
        return value
    }

    const baseDir = options.configPath ? path.dirname(options.configPath) : cwd
    return path.resolve(baseDir, value)
}

export { resolveConfiguredCookieFilePath, type ResolveConfiguredCookieFilePathOptions }
