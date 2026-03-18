enum MediaToolEnum {
    /**
     * Plain http downloader
     */
    DEFAULT = 'default',
    GALLERY_DL = 'gallery-dl',
    YT_DLP = 'yt-dlp',
}

type MediaToolConfigMap = {
    [MediaToolEnum.DEFAULT]: {}
    [MediaToolEnum.GALLERY_DL]: {
        /**
         * 可以为空，默认寻找系统路径中的 gallery-dl
         */
        path?: string
        cookie_file?: string
    }
    [MediaToolEnum.YT_DLP]: {
        /**
         * 可以为空，默认寻找系统路径中的 yt-dlp
         */
        path?: string
        cookie_file?: string
        format?: string
    }
}

type MediaTool<T extends MediaToolEnum = MediaToolEnum> = {
    tool: T
} & MediaToolConfigMap[T]

type MediaStorageType = 'no-storage'

interface Media {
    type: MediaStorageType
    use: MediaTool
    // TODO
    fallbacks?: Array<MediaTool | MediaToolEnum>
}

export type { Media, MediaTool, MediaToolConfigMap }
export { MediaToolEnum }
