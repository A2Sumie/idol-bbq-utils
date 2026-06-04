import crypto from 'crypto'
import fs from 'fs'
import path from 'path'
import type { AppConfig } from '@/types'
import { buildConfigAudit } from './config-audit-service'

function hashFile(filePath: string) {
    try {
        return crypto.createHash('sha256').update(fs.readFileSync(filePath)).digest('hex')
    } catch {
        return null
    }
}

function countMigrations(root: string) {
    const migrationsDir = path.join(root, 'prisma', 'migrations')
    try {
        return fs
            .readdirSync(migrationsDir, { withFileTypes: true })
            .filter((entry) => entry.isDirectory())
            .map((entry) => entry.name)
            .sort()
    } catch {
        return []
    }
}

function buildRuntimeManifest(configPath: string, config?: AppConfig) {
    const cwd = process.cwd()
    const appRoot = cwd.endsWith(path.join('app', 'tweet-forwarder')) ? cwd : path.join(cwd, 'app', 'tweet-forwarder')
    const schemaPath = path.join(appRoot, 'prisma', 'schema.prisma')
    const clientSchemaPath = path.join(appRoot, 'prisma', 'client', 'schema.prisma')
    const resolvedConfigPath = path.isAbsolute(configPath) ? configPath : path.join(cwd, configPath)
    const configAudit = config ? buildConfigAudit(config) : null
    return {
        generated_at: new Date().toISOString(),
        process: {
            cwd,
            bun_version: typeof Bun !== 'undefined' ? Bun.version : null,
            database_url: process.env.DATABASE_URL ? '[set]' : '[unset]',
        },
        config: {
            path: configPath,
            hash: configAudit?.redacted_config_hash || null,
            redacted_hash: configAudit?.redacted_config_hash || null,
            policy_hash: configAudit?.policy_hash || null,
            raw_file_hash_present: fs.existsSync(resolvedConfigPath),
            secret_field_count: configAudit?.secret_fields.count || 0,
            counts: {
                crawlers: config?.crawlers?.length || 0,
                processors: config?.processors?.length || 0,
                formatters: config?.formatters?.length || 0,
                targets: config?.forward_targets?.length || 0,
                forwarders: config?.forwarders?.length || 0,
            },
            route_graph_counts: configAudit?.route_graph.counts || null,
        },
        prisma: {
            schema_hash: hashFile(schemaPath),
            generated_client_schema_hash: hashFile(clientSchemaPath),
            migrations: countMigrations(appRoot),
        },
    }
}

export { buildRuntimeManifest }
