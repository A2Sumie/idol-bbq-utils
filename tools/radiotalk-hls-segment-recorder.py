#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import re
import sys
import time
import urllib.parse
import urllib.request
from pathlib import Path


API_BASE = "https://api.radiotalk.jp"
DEFAULT_USER_AGENT = "RT_AND/release/8.0.4/SHG01/12"


def now():
    return dt.datetime.now().isoformat(timespec="seconds")


def eprint(*args):
    print(*args, file=sys.stderr, flush=True)


def fetch(url, user_agent, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": user_agent})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        return resp.read(), resp.headers.get("content-type", "")


def fetch_json(url, user_agent, timeout=12):
    body, _ = fetch(url, user_agent, timeout=timeout)
    return json.loads(body.decode("utf-8"))


def playlist_lines(url, user_agent):
    body, ctype = fetch(url, user_agent)
    text = body.decode("utf-8", "replace")
    return [line.strip() for line in text.splitlines() if line.strip()], ctype


def pick_variant(master_url, user_agent):
    lines, _ = playlist_lines(master_url, user_agent)
    for idx, line in enumerate(lines):
        if line.startswith("#EXT-X-STREAM-INF") and idx + 1 < len(lines):
            return urllib.parse.urljoin(master_url, lines[idx + 1])
    return master_url


def media_segments(media_url, user_agent):
    lines, _ = playlist_lines(media_url, user_agent)
    endlist = any(line == "#EXT-X-ENDLIST" for line in lines)
    segments = []
    for line in lines:
        if line.startswith("#"):
            continue
        segments.append(urllib.parse.urljoin(media_url, line))
    return segments, endlist


def segment_id(url):
    parts = urllib.parse.urlsplit(url)
    return parts.path


def segment_number(url):
    match = re.search(r"/(\d+)\.aac$", segment_id(url))
    return int(match.group(1)) if match else None


def segment_sort_key(url):
    number = segment_number(url)
    return (0, number) if number is not None else (1, segment_id(url))


def log_line(path, event, **fields):
    row = {"ts": now(), "event": event, **fields}
    line = json.dumps(row, ensure_ascii=False, sort_keys=True)
    if path:
        with path.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    return row


def slug_text(value, max_len=48):
    value = re.sub(r"\s+", "_", str(value).strip())
    value = re.sub(r"[^0-9A-Za-z._-]+", "_", value)
    value = value.strip("._-")
    return (value or "live")[:max_len]


def active_live_for_program(program_id, user_agent, live_id=None, allow_membership=False):
    url = f"{API_BASE}/v2/programs/{program_id}/live"
    rows = fetch_json(url, user_agent)
    if not isinstance(rows, list):
        raise RuntimeError(f"unexpected response shape from {url}: {type(rows).__name__}")
    candidates = rows
    if live_id:
        candidates = [row for row in rows if row.get("id") == live_id]
    for row in candidates:
        if row.get("status") != "live":
            continue
        if row.get("is_membership_live") and not allow_membership:
            continue
        if row.get("live_url"):
            return row
    return None


def default_out_path(out_dir, live=None):
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = dt.datetime.now().strftime("%Y%m%d-%H%M%S")
    if live:
        live_id = str(live.get("id") or "live").split("-")[0]
        title = slug_text(live.get("title") or "live")
        return out_dir / f"radiotalk-segments-{live_id}-{stamp}-{title}.aac"
    return out_dir / f"radiotalk-segments-{stamp}.aac"


def resolve_master_url(args):
    if args.master_url:
        return args.master_url, None
    deadline = time.monotonic() + args.wait_timeout if args.wait_timeout > 0 else None
    while True:
        try:
            live = active_live_for_program(
                args.program_id,
                args.user_agent,
                live_id=args.live_id,
                allow_membership=args.allow_membership,
            )
            if live:
                return live.get("live_url"), live
        except Exception as exc:
            eprint(f"{now()} wait-error {exc!r}")
        if deadline is not None and time.monotonic() >= deadline:
            raise TimeoutError("timed out waiting for a public active live_url")
        eprint(f"{now()} waiting-for-live program_id={args.program_id}")
        time.sleep(args.wait_interval)


def record_segments(master_url, out_path, log_path, args):
    out_path.parent.mkdir(parents=True, exist_ok=True)
    if log_path:
        log_path.parent.mkdir(parents=True, exist_ok=True)
    seen = set()
    media_url = None
    started = time.monotonic()
    last_status = started
    last_variant_refresh = 0.0
    total = 0
    seq = 0
    last_number = None
    missing_segments = 0
    head_missing_segments = 0
    log_line(log_path, "start", master_url=master_url, out=str(out_path))
    eprint(f"{now()} start out={out_path}")

    with out_path.open("ab", buffering=0) as out:
        while time.monotonic() - started <= args.max_seconds:
            try:
                now_mono = time.monotonic()
                if media_url is None or now_mono - last_variant_refresh >= args.variant_refresh_interval:
                    media_url = pick_variant(master_url, args.user_agent)
                    log_line(log_path, "variant", media_url=media_url)
                    last_variant_refresh = now_mono
                urls, endlist = media_segments(media_url, args.user_agent)
                new_urls = [url for url in urls if segment_id(url) not in seen and segment_id(url) != "/gap.aac"]
                new_urls.sort(key=segment_sort_key)
                for url in new_urls:
                    data, ctype = fetch(url, args.user_agent)
                    if not data:
                        continue
                    number = segment_number(url)
                    previous_number = last_number if last_number is not None else 0
                    if number is not None and number > previous_number + 1:
                        missing = number - previous_number - 1
                        if previous_number == 0:
                            # Head gap: HLS sliding window at join makes the pre-join head
                            # unavailable by design; priced into the title via head-loss logic.
                            head_missing_segments += missing
                            log_line(
                                log_path,
                                "head_gap",
                                previous_segment=0,
                                current_segment=number,
                                missing=missing,
                                head_missing_segments=head_missing_segments,
                            )
                            eprint(f"{now()} head-gap previous=0 current={number} missing={missing}")
                        else:
                            missing_segments += missing
                            log_line(
                                log_path,
                                "gap",
                                previous_segment=previous_number,
                                current_segment=number,
                                missing=missing,
                                missing_segments=missing_segments,
                            )
                            eprint(f"{now()} gap previous={previous_number} current={number} missing={missing}")
                    out.write(data)
                    out.flush()
                    seen.add(segment_id(url))
                    if number is not None:
                        last_number = number
                    seq += 1
                    total += len(data)
                    log_line(
                        log_path,
                        "segment",
                        seq=seq,
                        bytes=len(data),
                        total_bytes=total,
                        ctype=ctype,
                        segment_id=segment_id(url),
                        url=url,
                    )
                now_mono = time.monotonic()
                if args.status_interval > 0 and now_mono - last_status >= args.status_interval:
                    eprint(f"{now()} status segments={seq} total_bytes={total} endlist={endlist}")
                    last_status = now_mono
                if endlist:
                    log_line(
                        log_path,
                        "endlist",
                        segments=seq,
                        total_bytes=total,
                        missing_segments=missing_segments,
                        head_missing_segments=head_missing_segments,
                    )
                    eprint(
                        f"{now()} endlist segments={seq} total_bytes={total} "
                        f"missing_segments={missing_segments} "
                        f"head_missing_segments={head_missing_segments}"
                    )
                    return 2 if missing_segments else 0
            except KeyboardInterrupt:
                log_line(log_path, "interrupt", segments=seq, total_bytes=total)
                return 130
            except Exception as exc:
                log_line(log_path, "error", error=repr(exc), segments=seq, total_bytes=total)
                eprint(f"{now()} error {exc!r}")
            time.sleep(args.poll_interval)
    log_line(
        log_path,
        "timeout",
        segments=seq,
        total_bytes=total,
        missing_segments=missing_segments,
        head_missing_segments=head_missing_segments,
    )
    eprint(
        f"{now()} timeout segments={seq} total_bytes={total} "
        f"missing_segments={missing_segments} "
        f"head_missing_segments={head_missing_segments}"
    )
    return 2 if missing_segments else 0


def main():
    parser = argparse.ArgumentParser(
        description="Record a Radiotalk HLS audio live by appending audio/aac segments."
    )
    parser.add_argument("--master-url", help="HLS master URL; skips Radiotalk API polling")
    parser.add_argument("--program-id", type=int, default=97270)
    parser.add_argument("--live-id", help="optional expected Radiotalk live id")
    parser.add_argument("--allow-membership", action="store_true")
    parser.add_argument("--wait-timeout", type=float, default=0.0, help="seconds; 0 waits forever")
    parser.add_argument("--wait-interval", type=float, default=5.0)
    parser.add_argument("--poll-interval", type=float, default=0.5)
    parser.add_argument("--variant-refresh-interval", type=float, default=30.0)
    parser.add_argument("--status-interval", type=float, default=30.0)
    parser.add_argument("--max-seconds", type=float, default=14400.0)
    parser.add_argument("--out-dir", type=Path, default=Path("captures/radiotalk"))
    parser.add_argument("--out", type=Path)
    parser.add_argument("--log-jsonl", type=Path)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    args = parser.parse_args()

    master_url, live = resolve_master_url(args)
    out_path = args.out or default_out_path(args.out_dir, live)
    log_path = args.log_jsonl or out_path.with_suffix(".jsonl")
    meta_path = out_path.with_suffix(".meta.json")
    meta_path.write_text(
        json.dumps(
            {
                "created_at": now(),
                "program_id": args.program_id,
                "live_id": args.live_id,
                "live": live,
                "master_url": master_url,
                "out": str(out_path),
                "log_jsonl": str(log_path),
                "mode": "radiotalk-hls-segment-recorder",
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    eprint(f"{now()} master_url={master_url}")
    eprint(f"{now()} meta={meta_path}")
    return record_segments(master_url, out_path, log_path, args)


if __name__ == "__main__":
    raise SystemExit(main())
