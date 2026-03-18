import { expect, test } from 'bun:test'
import { resolveBatchTargetIds } from './forwarder-manager'

test('resolveBatchTargetIds skips targets with bypass_batch enabled', () => {
    const targetIds = resolveBatchTargetIds(
        ['formatter-a', 'formatter-b'],
        {
            'formatter-a': ['group-1', 'group-2'],
            'formatter-b': ['group-2', 'group-3'],
        },
        [
            {
                id: 'group-1',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '1',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                } as any,
            },
            {
                id: 'group-2',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '2',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                    bypass_batch: true,
                } as any,
            },
            {
                id: 'group-3',
                platform: 'qq' as any,
                cfg_platform: {
                    group_id: '3',
                    token: '',
                    url: 'http://127.0.0.1:3001',
                } as any,
            },
        ] as any,
    )

    expect(targetIds).toEqual(['group-1', 'group-3'])
})
