#!/usr/bin/env bun
/**
 * Config Migration Script
 * 
 * Migrates config.yaml to new format with independent Formatters and Translators
 * 
 * Usage: bun migrate-config.ts [input] [output]
 *   input: path to config.yaml (default: ./config.yaml)
 *   output: path to save migrated config (default: ./config.migrated.yaml)
 */

import YAML from 'yaml';
import fs from 'fs';
import path from 'path';
import crypto from 'crypto';

interface OldConfig {
    crawlers?: Array<any>;
    forwarders?: Array<any>;
    forward_targets?: Array<any>;
    [key: string]: any;
}

interface NewConfig extends OldConfig {
    translators?: Array<any>;
    formatters?: Array<any>;
    connections?: {
        'crawler-translator'?: Record<string, string>;
        'translator-formatter'?: Record<string, string[]>;
        'crawler-formatter'?: Record<string, string[]>;
        'formatter-target'?: Record<string, string[]>;
    };
}

function generateId(prefix: string, name?: string): string {
    if (name) {
        return `${prefix}-${name.toLowerCase().replace(/[^a-z0-9]/g, '-')}`;
    }
    return `${prefix}-${crypto.randomBytes(4).toString('hex')}`;
}

function migrateConfig(oldConfig: OldConfig): NewConfig {
    const newConfig: NewConfig = { ...oldConfig };

    // Initialize new arrays and connections
    newConfig.translators = newConfig.translators || [];
    newConfig.formatters = newConfig.formatters || [];
    newConfig.connections = newConfig.connections || {
        'crawler-translator': {},
        'translator-formatter': {},
        'crawler-formatter': {},
        'formatter-target': {},
    };

    const extractedTranslators = new Map<string, any>();
    const extractedFormatters = new Map<string, any>();

    console.log('🔄 Starting migration...\n');

    // Phase 1: Extract Translators from Crawlers
    console.log('📝 Phase 1: Extracting Translators from Crawlers');
    if (oldConfig.crawlers) {
        oldConfig.crawlers.forEach((crawler, index) => {
            const crawlerId = crawler.name || `crawler-${index}`;

            if (crawler.cfg_crawler?.translator) {
                const translator = crawler.cfg_crawler.translator;
                const translatorId = generateId('translator', translator.provider);

                // Add to extracted translators if not already present
                const translatorKey = JSON.stringify(translator);
                if (!extractedTranslators.has(translatorKey)) {
                    extractedTranslators.set(translatorKey, {
                        id: translatorId,
                        name: `${translator.provider} Translator`,
                        ...translator,
                    });
                    console.log(`  ✓ Extracted translator from crawler "${crawlerId}": ${translatorId}`);
                }

                // Create connection
                const actualTranslatorId = extractedTranslators.get(translatorKey)!.id;
                newConfig.connections!['crawler-translator']![crawlerId] = actualTranslatorId;

                // Remove embedded translator (mark as legacy by adding translator_id reference)
                if (newConfig.crawlers![index].cfg_crawler) {
                    newConfig.crawlers![index].cfg_crawler.translator_id = actualTranslatorId;
                    // Comment: Keep old translator for backward compatibility
                    // delete newConfig.crawlers![index].cfg_crawler.translator;
                }
            }
        });
    }

    // Phase 2: Extract Formatters from Forwarders
    console.log('\n📝 Phase 2: Extracting Formatters from Forwarders');
    if (oldConfig.forwarders) {
        oldConfig.forwarders.forEach((forwarder, index) => {
            const forwarderId = forwarder.name || `forwarder-${index}`;

            if (forwarder.cfg_forwarder?.render_type) {
                const renderType = forwarder.cfg_forwarder.render_type;
                const formatterId = generateId('formatter', renderType);

                // Add to extracted formatters if not already present
                if (!extractedFormatters.has(renderType)) {
                    extractedFormatters.set(renderType, {
                        id: formatterId,
                        name: `${renderType.toUpperCase()} Formatter`,
                        render_type: renderType,
                    });
                    console.log(`  ✓ Extracted formatter from forwarder "${forwarderId}": ${formatterId} (${renderType})`);
                }

                // Add formatter_id reference
                if (newConfig.forwarders![index].cfg_forwarder) {
                    newConfig.forwarders![index].cfg_forwarder.formatter_id = extractedFormatters.get(renderType)!.id;
                }
            }
        });
    }

    // Phase 3: Build Formatter-Target connections
    console.log('\n📝 Phase 3: Building Formatter-Target connections');
    if (oldConfig.forwarders && oldConfig.forward_targets) {
        oldConfig.forwarders.forEach((forwarder, index) => {
            const formatterRenderType = forwarder.cfg_forwarder?.render_type;
            if (!formatterRenderType) return;

            const formatterId = extractedFormatters.get(formatterRenderType)!.id;

            // Get subscriber targets
            const subscribers = forwarder.subscribers || [];
            subscribers.forEach((sub: any) => {
                const targetId = typeof sub === 'string' ? sub : sub.id;

                if (!newConfig.connections!['formatter-target']![formatterId]) {
                    newConfig.connections!['formatter-target']![formatterId] = [];
                }
                if (!newConfig.connections!['formatter-target']![formatterId].includes(targetId)) {
                    newConfig.connections!['formatter-target']![formatterId].push(targetId);
                    console.log(`  ✓ Connected formatter "${formatterId}" to target "${targetId}"`);
                }
            });
        });
    }

    // Phase 4: Build Translator-Formatter connections (based on forwarder associations)
    console.log('\n📝 Phase 4: Building Translator-Formatter connections');
    if (oldConfig.crawlers && oldConfig.forwarders) {
        oldConfig.forwarders.forEach((forwarder) => {
            const formatterRenderType = forwarder.cfg_forwarder?.render_type;
            if (!formatterRenderType) return;

            const formatterId = extractedFormatters.get(formatterRenderType)!.id;

            // Find matching crawlers by website/origin
            const matchingCrawlers = oldConfig.crawlers!.filter(crawler => {
                if (forwarder.websites) {
                    return forwarder.websites.some((fw: string) =>
                        crawler.websites?.some((cw: string) => cw.includes(fw) || fw.includes(cw))
                    );
                }
                if (forwarder.origin && crawler.websites) {
                    return crawler.websites.some((cw: string) => cw.includes(forwarder.origin));
                }
                return false;
            });

            matchingCrawlers.forEach(crawler => {
                const crawlerId = crawler.name || `crawler-${oldConfig.crawlers!.indexOf(crawler)}`;
                const translatorId = newConfig.connections!['crawler-translator']![crawlerId];

                if (translatorId) {
                    if (!newConfig.connections!['translator-formatter']![translatorId]) {
                        newConfig.connections!['translator-formatter']![translatorId] = [];
                    }
                    if (!newConfig.connections!['translator-formatter']![translatorId].includes(formatterId)) {
                        newConfig.connections!['translator-formatter']![translatorId].push(formatterId);
                        console.log(`  ✓ Connected translator "${translatorId}" to formatter "${formatterId}"`);
                    }
                }
            });
        });
    }

    // Add extracted items to config
    newConfig.translators!.push(...Array.from(extractedTranslators.values()));
    newConfig.formatters!.push(...Array.from(extractedFormatters.values()));

    return newConfig;
}

// Main execution
const inputPath = process.argv[2] || path.join(process.cwd(), 'config.yaml');
const outputPath = process.argv[3] || path.join(process.cwd(), 'config.migrated.yaml');

console.log('═══════════════════════════════════════════════════════════');
console.log('  Config Migration Tool');
console.log('═══════════════════════════════════════════════════════════\n');
console.log(`📂 Input:  ${inputPath}`);
console.log(`📂 Output: ${outputPath}\n`);

try {
    // Read and parse old config
    const yamlContent = fs.readFileSync(inputPath, 'utf-8');
    const oldConfig: OldConfig = YAML.parse(yamlContent);

    // Migrate
    const newConfig = migrateConfig(oldConfig);

    // Save migrated config
    const newYaml = YAML.stringify(newConfig, {
        indent: 2,
        lineWidth: 0,
    });
    fs.writeFileSync(outputPath, newYaml, 'utf-8');

    // Summary
    console.log('\n═══════════════════════════════════════════════════════════');
    console.log('✅ Migration completed successfully!\n');
    console.log(`📊 Summary:`);
    console.log(`   - Translators extracted: ${newConfig.translators?.length || 0}`);
    console.log(`   - Formatters extracted: ${newConfig.formatters?.length || 0}`);
    console.log(`   - Connections created:`);
    console.log(`     • Crawler→Translator: ${Object.keys(newConfig.connections?.['crawler-translator'] || {}).length}`);
    console.log(`     • Translator→Formatter: ${Object.keys(newConfig.connections?.['translator-formatter'] || {}).length}`);
    console.log(`     • Formatter→Target: ${Object.keys(newConfig.connections?.['formatter-target'] || {}).length}`);
    console.log(`\n📝 Next steps:`);
    console.log(`   1. Review the migrated config: ${outputPath}`);
    console.log(`   2. Backup your current config: cp config.yaml config.yaml.backup`);
    console.log(`   3. Replace with migrated version: cp ${path.basename(outputPath)} config.yaml`);
    console.log(`   4. Restart the server to apply changes`);
    console.log('═══════════════════════════════════════════════════════════\n');

} catch (error) {
    console.error('\n❌ Migration failed:', error);
    process.exit(1);
}
