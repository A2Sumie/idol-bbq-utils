#!/usr/bin/env python3

import argparse
import json
import sys

from biliup.engine.upload import UploadBase
from biliup.plugins.bili_webup import BiliWeb


def parse_args():
    parser = argparse.ArgumentParser(description="Upload videos to Bilibili via biliup.")
    parser.add_argument("--cookie-file", required=True)
    parser.add_argument("--title", required=True)
    parser.add_argument("--desc", default="")
    parser.add_argument("--source-url", required=True)
    parser.add_argument("--cover", default="")
    parser.add_argument("--submit-api", default="web")
    parser.add_argument("--tid", type=int, default=171)
    parser.add_argument("--threads", type=int, default=3)
    parser.add_argument("--copyright", type=int, choices=(1, 2), default=2)
    parser.add_argument("--tag", action="append", default=[])
    parser.add_argument("files", nargs="+")
    return parser.parse_args()


def main():
    args = parse_args()
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
        copyright=args.copyright,
        threads=args.threads,
        tid=args.tid,
        tags=args.tag,
        cover_path=args.cover or None,
        description=args.desc,
    )
    file_list = [UploadBase.FileInfo(video=video_path, danmaku=None) for video_path in args.files]
    uploader.upload(file_list)
    print(
        json.dumps(
            {
                "ok": True,
                "title": args.title,
                "files": args.files,
                "tid": args.tid,
            },
            ensure_ascii=False,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(str(exc), file=sys.stderr)
        raise
