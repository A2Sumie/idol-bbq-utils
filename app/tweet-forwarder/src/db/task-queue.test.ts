import { expect, test } from 'bun:test'
import DB from '@/db'

test('TaskQueue idempotent add only revives failed existing tasks', () => {
    expect(DB.TaskQueue.shouldReviveExistingTaskOnAdd({ status: 'failed' })).toBeTrue()

    for (const status of ['pending', 'processing', 'completed', 'cancelled']) {
        expect(DB.TaskQueue.shouldReviveExistingTaskOnAdd({ status })).toBeFalse()
    }
})
