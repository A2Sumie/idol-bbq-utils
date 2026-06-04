import { expect, test } from 'bun:test'
import { TaskScheduler } from './base'

test('TaskScheduler payload guards reject malformed event payloads', () => {
    expect(TaskScheduler.isTaskCtx(undefined)).toBeFalse()
    expect(
        TaskScheduler.isTaskCtx({
            taskId: 'task-1',
            task: {
                id: 'task-1',
                status: 'queued',
                data: {},
            },
        }),
    ).toBeFalse()
    expect(
        TaskScheduler.isTaskCtx({
            taskId: 'task-1',
            task: {
                id: 'task-1',
                status: TaskScheduler.TaskStatus.PENDING,
            },
        }),
    ).toBeFalse()
    expect(
        TaskScheduler.isTaskStatusPayload({
            taskId: 'task-1',
            status: 'done',
        }),
    ).toBeFalse()
    expect(
        TaskScheduler.isTaskFinishedPayload({
            taskId: 'task-1',
            result: null,
        }),
    ).toBeFalse()
})

test('TaskScheduler payload guards accept valid task event payloads', () => {
    expect(
        TaskScheduler.isTaskCtx({
            taskId: 'task-1',
            task: {
                id: 'task-1',
                status: TaskScheduler.TaskStatus.PENDING,
                data: {},
            },
        }),
    ).toBeTrue()
    expect(
        TaskScheduler.isTaskStatusPayload({
            taskId: 'task-1',
            status: TaskScheduler.TaskStatus.RUNNING,
        }),
    ).toBeTrue()
    expect(
        TaskScheduler.isTaskFinishedPayload({
            taskId: 'task-1',
            result: [],
        }),
    ).toBeTrue()
})
