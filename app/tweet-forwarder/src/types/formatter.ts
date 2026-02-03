/**
 * Supported render types for formatting articles
 */
type RenderType = 'text' | 'img' | 'tag' | 'img-tag' | 'img-tag-dynamic'

/**
 * Formatter type definition for independent formatter configuration
 */
interface Formatter {
    id?: string
    name?: string
    group?: string
    render_type: RenderType
}

/**
 * Visual connection type for Config UI graph
 */
type ConnectionType = 'crawler-translator' | 'translator-formatter' | 'crawler-formatter' | 'formatter-target'

/**
 * Connection mapping for multi-to-multi relationships between nodes
 */
interface ConnectionMap {
    'crawler-translator'?: Record<string, string>
    'translator-formatter'?: Record<string, string[]>
    'crawler-formatter'?: Record<string, string[]>
    'formatter-target'?: Record<string, string[]>
    'forwarder-target'?: Record<string, string[]>
}

export type { Formatter, ConnectionType, ConnectionMap }
