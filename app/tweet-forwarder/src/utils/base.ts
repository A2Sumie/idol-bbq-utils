import { Logger } from '@idol-bbq-utils/log'
import { CronJob } from 'cron'
import { EventEmitter } from 'events'
import { uniq } from 'lodash'

interface Droppable {
    drop(...args: any[]): Promise<void>
}

interface Stoppable {
    stop?(...args: any[]): Promise<void> | void
}

function isRecord(value: unknown): value is Record<string, unknown> {
    return typeof value === 'object' && value !== null
}

abstract class BaseCompatibleModel implements Droppable, Stoppable {
    abstract NAME: string
    protected abstract log?: Logger

    abstract init(...args: any[]): Promise<void>
    stop?(...args: any[]): Promise<void> | void
    abstract drop(...args: any[]): Promise<void>
}

namespace TaskScheduler {
    export enum TaskStatus {
        PENDING = 'pending',
        RUNNING = 'running',
        COMPLETED = 'completed',
        CANCELLED = 'cancelled',
        FAILED = 'failed',
    }

    export interface Task {
        id: string
        status: TaskStatus
        data: any
        meta?: Record<string, unknown>
    }

    export interface TaskCtx {
        taskId: string
        task: Task
        log?: Logger
    }

    export interface TaskStatusPayload {
        taskId: string
        status: TaskStatus
    }

    export interface TaskFinishedPayload<T = unknown> {
        taskId: string
        result: Array<T>
        immediate_notify?: boolean
        crawlerName?: string
    }

    export enum TaskEvent {
        DISPATCH = 'task:dispatch',
        UPDATE_STATUS = 'task:update-status',
        FINISHED = 'task:finished',
    }

    export function isTaskStatus(value: unknown): value is TaskStatus {
        return Object.values(TaskStatus).includes(value as TaskStatus)
    }

    export function isTask(value: unknown): value is Task {
        return (
            isRecord(value) &&
            typeof value.id === 'string' &&
            isTaskStatus(value.status) &&
            Object.prototype.hasOwnProperty.call(value, 'data')
        )
    }

    export function isTaskCtx(value: unknown): value is TaskCtx {
        return isRecord(value) && typeof value.taskId === 'string' && isTask(value.task)
    }

    export function isTaskStatusPayload(value: unknown): value is TaskStatusPayload {
        return isRecord(value) && typeof value.taskId === 'string' && isTaskStatus(value.status)
    }

    export function isTaskFinishedPayload<T = unknown>(value: unknown): value is TaskFinishedPayload<T> {
        return isRecord(value) && typeof value.taskId === 'string' && Array.isArray(value.result)
    }

    export abstract class TaskScheduler extends BaseCompatibleModel {
        protected emitter: EventEmitter
        protected tasks: Map<string, Task> = new Map()
        protected cronJobs: Array<CronJob> = []
        protected taskHandlers: Record<Exclude<TaskEvent, TaskEvent.DISPATCH>, (...args: any[]) => void>

        constructor(emitter: EventEmitter) {
            super()
            this.taskHandlers = {
                [TaskEvent.UPDATE_STATUS]: this.updateTaskStatus.bind(this),
                [TaskEvent.FINISHED]: this.finishTask.bind(this),
            }
            this.emitter = emitter
        }
        abstract start(...args: any[]): Promise<void>
        abstract stop(...args: any[]): Promise<void>
        abstract drop(...args: any[]): Promise<void>
        abstract updateTaskStatus(...args: any[]): void
        abstract finishTask(...args: any[]): void

        getActiveTaskCount() {
            return this.tasks.size
        }
    }
}

/**
 * Sanitize websites, origin and paths to a list of websites.
 *
 * return websites if provided, otherwise return a list of websites constructed from origin and paths.
 */
function sanitizeWebsites({
    websites,
    origin,
    paths,
}: {
    websites?: Array<string>
    origin?: string
    paths?: Array<string>
}): Array<string> {
    let res = [] as Array<string>
    if (websites) {
        res = res.concat(websites)
    }
    if (origin) {
        if (paths && paths.length > 0) {
            res = res.concat(paths.map((p) => `${origin.replace(/\/$/, '')}/${p.replace(/^\//, '')}`))
        }
    }
    return uniq(res)
}

export { TaskScheduler, BaseCompatibleModel, sanitizeWebsites }
