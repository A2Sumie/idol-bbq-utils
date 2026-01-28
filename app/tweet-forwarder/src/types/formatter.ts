/**
 * Formatter type definition for independent formatter configuration
 */
interface Formatter {
    id?: string
    name?: string
    render_type: 'text' | 'img' | 'img-with-meta' | 'img-with-source' | 'img-with-source-summary'
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
