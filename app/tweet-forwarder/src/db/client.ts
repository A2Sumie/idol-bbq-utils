import { PrismaClient, Prisma } from '../../prisma/client/index.js'

type PrismaClientOptions = ConstructorParameters<typeof PrismaClient>[0]
export type PrismaClientInstance = InstanceType<typeof PrismaClient>

export function createPrismaClient(options?: PrismaClientOptions) {
    return new PrismaClient(options)
}

let prisma = createPrismaClient()

export function setPrismaForTesting(next: PrismaClientInstance) {
    prisma = next
}

export { prisma, Prisma }
