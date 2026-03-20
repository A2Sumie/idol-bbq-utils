import { Input, Telegraf } from 'telegraf'
import { Forwarder, type SendProps } from './base'
import type { InputMediaPhoto, InputMediaVideo } from 'telegraf/types'
import { chunk } from 'lodash'
import { type ForwardTargetPlatformConfig, ForwardTargetPlatformEnum } from '@/types/forwarder'

class TgForwarder extends Forwarder {
    static _PLATFORM = ForwardTargetPlatformEnum.Telegram
    protected override BASIC_TEXT_LIMIT = 1024
    NAME = 'telegram'
    private chat_id: string
    private bot: Telegraf

    constructor(...[config, ...rest]: [...ConstructorParameters<typeof Forwarder>]) {
        super(config, ...rest)
        const { chat_id, token } = config as ForwardTargetPlatformConfig<ForwardTargetPlatformEnum.Telegram>
        if (!chat_id || !token) {
            throw new Error(`forwarder ${this.NAME} chat_id and bot token is required`)
        }
        this.chat_id = chat_id
        this.bot = new Telegraf(token)
    }

    protected async realSend(texts: string[], props?: SendProps): Promise<any> {
        const { media } = props || {}
        if (media && media.length !== 0) {
            const mediaGroups = chunk(
                media
                    .map((item) => {
                        if (item.media_type === 'photo' || item.media_type === 'video_thumbnail') {
                            return {
                                media: Input.fromLocalFile(item.path),
                                type: 'photo' as InputMediaPhoto['type'],
                            }
                        }
                        if (item.media_type === 'video') {
                            return {
                                media: Input.fromLocalFile(item.path),
                                type: 'video' as InputMediaVideo['type'],
                            }
                        }
                        return undefined
                    })
                    .filter((item) => item !== undefined),
                10,
            )

            const [firstText, ...remainingTexts] = texts

            for (const [index, group] of mediaGroups.entries()) {
                await this.bot.telegram.sendMediaGroup(
                    this.chat_id,
                    group.map((item, itemIndex) => ({
                        ...item,
                        caption: index === 0 && itemIndex === 0 ? firstText : undefined,
                    })),
                )
            }

            for (const text of remainingTexts) {
                await this.bot.telegram.sendMessage(this.chat_id, text)
            }
            return
        }

        for (const text of texts) {
            await this.bot.telegram.sendMessage(this.chat_id, text)
        }
        return
    }
}

export { TgForwarder }
