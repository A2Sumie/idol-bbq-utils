#!/usr/bin/env python3

import argparse
import json
import sys
import time

from biliup.engine.upload import UploadBase
from biliup.plugins.bili_webup import BiliBili, BiliWeb, Data

DEFAULT_BROWSER_UA = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/146.0.0.0 Safari/537.36"
)


def patch_biliup_headers(user_agent: str):
    original_init = BiliBili.__init__

    def patched_init(self, video):
        original_init(self, video)
        session = getattr(self, "_BiliBili__session", None)
        if session is None:
            return
        session.headers.update(
            {
                "user-agent": user_agent,
                "referer": "https://www.bilibili.com/",
                "origin": "https://www.bilibili.com",
                "accept": "application/json, text/plain, */*",
            }
        )

    BiliBili.__init__ = patched_init


def parse_args():
    parser = argparse.ArgumentParser(description="Upload videos to Bilibili via biliup.")
    parser.add_argument("--cookie-file", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--desc", default="")
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--cover", default="")
    parser.add_argument("--submit-api", default="web")
    parser.add_argument("--line", default="AUTO")
    parser.add_argument("--tid", type=int, default=171)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--copyright", type=int, choices=(1, 2), default=2)
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("--user-agent", default=DEFAULT_BROWSER_UA)
    parser.add_argument("files", nargs="+")
    return parser.parse_args()


def main():
    args = parse_args()
    patch_biliup_headers(args.user_agent)
    uploader = BiliWeb(
        principal="idol-bbq-utils",
        data={
            "format_title": args.title,
            "url": args.source_url,
            "name": "idol-bbq-utils",
        },
        user={},
        user_cookie=args.cookie_file,
        submit_api=args.submit_api,
        lines=args.line,
        copyright=args.copyright,
        threads=args.threads,
        tid=args.tid,
        tags=args.tag,
        cover_path=args.cover or None,
        description=args.desc,
    )
    file_list = [UploadBase.FileInfo(video=video_path, danmaku=None) for video_path in args.files]
    submit_result = upload_and_submit(uploader, file_list)
    print(
        json.dumps(
            {
                "ok": True,
                "title": args.title,
                "files": args.files,
                "tid": args.tid,
                "submit_result": submit_result,
            },
            ensure_ascii=False,
        )
    )


def upload_and_submit(uploader: BiliWeb, file_list):
    video = Data()
    video.dynamic = uploader.dynamic
    with BiliBili(video) as bili:
        bili.app_key = uploader.user.get("app_key")
        bili.appsec = uploader.user.get("appsec")
        bili.login(uploader.persistence_path, uploader.user_cookie)
        for file in file_list:
            video_part = bili.upload_file(file.video, uploader.lines, uploader.threads)
            video_part["title"] = video_part["title"][:80]
            video.append(video_part)
        video.title = uploader.data["format_title"][:80]
        if uploader.credits:
            video.desc_v2 = uploader.creditsToDesc_v2()
        else:
            video.desc_v2 = [
                {
                    "raw_text": uploader.desc,
                    "biz_id": "",
                    "type": 1,
                }
            ]
        video.desc = uploader.desc
        video.copyright = uploader.copyright
        if uploader.copyright == 2:
            video.source = uploader.data["url"]
        video.tid = uploader.tid
        video.set_tag(uploader.tags)
        if uploader.dtime:
            video.delay_time(int(time.time()) + uploader.dtime)
        if uploader.cover_path:
            video.cover = bili.cover_up(uploader.cover_path).replace("http:", "")
        return bili.submit(uploader.submit_api)


def explain_upload_error(exc: Exception) -> str:
    message = str(exc)
    if message == "'chunk_size'":
        return (
            "biliup preupload response did not contain chunk_size; "
            "Bilibili likely rejected preupload first (common causes: upload rate limit code 601 or stale upload line)"
        )
    return message


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(explain_upload_error(exc), file=sys.stderr)
        raise
