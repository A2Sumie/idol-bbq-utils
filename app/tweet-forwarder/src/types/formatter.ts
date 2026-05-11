/**
 * Supported render types for formatting articles
 */
type RenderType =
    | 'text'
    | 'text-card'
    | 'text-compact'
    | 'text-compact-card'
    | 'img'
    | 'tag'
    | 'img-tag'
    | 'img-tag-dynamic'
    | 'img-with-meta'

/**
 * Formatter type definition for independent formatter configuration
 */
interface Formatter {
    id?: string
    name?: string
    group?: string
    render_type: RenderType
    /**
     * Enable hourly batch aggregation for this formatter
     */
    aggregation?: boolean
    /**
     * Enable media deduplication (skip if media already sent)
     */
    deduplication?: boolean
    /**
     * Feature flags layered onto this formatter's text behavior.
     */
    render_features?: Array<string>
    /**
     * Feature flags layered onto this formatter's rendered card template.
     */
    card_features?: Array<string>
}

/**
 * Visual connection type for Config UI graph
 */
type ConnectionType =
    | 'crawler-processor'
    | 'processor-formatter'
    | 'crawler-formatter'
    | 'formatter-target'
    | 'forwarder-target'

/**
 * Connection mapping for multi-to-multi relationships between nodes
 */
interface ConnectionMap {
    'crawler-processor'?: Record<string, string>
    'processor-formatter'?: Record<string, string[]>
    'crawler-formatter'?: Record<string, string[]>
    'formatter-target'?: Record<string, string[]>
    'forwarder-target'?: Record<string, string[]>
}

export type { Formatter, ConnectionType, ConnectionMap }
