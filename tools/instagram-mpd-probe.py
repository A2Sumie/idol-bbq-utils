#!/usr/bin/env python3
"""Instagram post_live/replay MPD probe (24h back-capture, timeliness-first).

Every INTERVAL (default 30 minutes) this probe checks the monitored IG handles for
post_live (replay) broadcasts via the private API. When one is found within the
replay window:

  1. Saves the broadcast info (published_time / status / dash URLs) immediately.
  2. Fetches live/<id>/info/ (the full live info incl. dash manifest) — the "before" snapshot.
  3. Downloads the FULL replay from the MPD (DASH segment downloader, instalive.py-style:
     init segments + every timeline segment, concat, ffmpeg merge).
  4. Re-fetches the live info after the download — the "after" snapshot.

Everything is written to <out_dir>/<handle>/<broadcast_id>/ (MPD, segments, info JSONs,
merged.mp4). The probe is designed to be run from cron every 30 minutes (--once with a
flock guard) so a replay that IG publishes within the 24h window is caught within half
an hour.

Usage:
  instagram-mpd-probe.py [--handles a,b] [--once] [--interval 1800] [--out-dir DIR]
"""
import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
import urllib.request
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.parse import urljoin

COOKIE_FILE = '/app/assets/cookies/inscks0318.txt'
SESSION_CACHE = '/app/ig-probe/session.json'
UID_CACHE = '/app/ig-probe/uids.json'
DEFAULT_HANDLES = ['nao_aikawa227', 'shiina_satsuki227']


def load_cookies(path):
    cookies = {}
    try:
        with open(path) as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith('#') or '\t' not in line:
                    continue
                parts = line.split('\t')
                if len(parts) == 7:
                    cookies[parts[5]] = parts[6]
    except Exception as exc:
        print(f'[probe] cookie load failed: {exc}')
    return cookies


def get_client():
    from instagrapi import Client
    cl = Client()
    cookies = load_cookies(COOKIE_FILE)
    sid = cookies.get('sessionid', '')
    if not sid:
        raise RuntimeError('no sessionid in cookie file')
    if os.path.exists(SESSION_CACHE):
        cl.load_settings(SESSION_CACHE)
    cl.login_by_sessionid(sid)
    cl.dump_settings(SESSION_CACHE)
    return cl


class DashReplayDownloader:
    """Minimal static-DASH replay downloader (instalive.py-style)."""

    def __init__(self, mpd_url, output_dir, timeout=30):
        self.mpd_url = mpd_url
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.session = None
        self.timeout = timeout

    def _fetch(self, url, binary=False):
        import requests
        if self.session is None:
            self.session = requests.Session()
            self.session.headers['User-Agent'] = (
                'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36'
            )
        r = self.session.get(url, timeout=self.timeout)
        r.raise_for_status()
        return r.content if binary else r.text

    def download(self):
        mpd_text = self._fetch(self.mpd_url)
        (self.output_dir / 'mpd.mpd').write_text(mpd_text, encoding='utf-8')
        root = ET.fromstring(mpd_text)
        ns = {'mpd': 'urn:mpeg:dash:schema:mpd:2011'}
        period = root.find('.//mpd:Period', ns) or root.find('Period')
        adaptations = period.findall('AdaptationSet') if period is not None else []

        def select_adaptation(content_type):
            for ad in adaptations:
                if ad.get('contentType') == content_type:
                    return ad
            return None

        video_ad = select_adaptation('video') or next((a for a in adaptations if a.get('mimeType', '').startswith('video')), None)
        audio_ad = select_adaptation('audio') or next((a for a in adaptations if a.get('mimeType', '').startswith('audio')), None)
        if video_ad is None:
            print(f'[dash] no video adaptation set in {self.mpd_url}')
            return None

        def download_adaptation(ad, kind):
            tmpl = ad.find('SegmentTemplate')
            if tmpl is None:
                print(f'[dash] no SegmentTemplate for {kind}')
                return []
            init_url = urljoin(self.mpd_url, tmpl.get('initialization', ''))
            media_url = urljoin(self.mpd_url, tmpl.get('media', ''))
            timeline = tmpl.find('SegmentTimeline')
            out_dir = self.output_dir / kind
            out_dir.mkdir(parents=True, exist_ok=True)
            files = []
            if init_url:
                init_path = out_dir / 'init.mp4'
                if not init_path.exists():
                    init_path.write_bytes(self._fetch(init_url, binary=True))
                    print(f'[dash] {kind} init downloaded')
                files.append(init_path)
            ts = []
            if timeline is not None:
                t = None
                for seg in timeline:
                    st = seg.get('t')
                    if st is not None:
                        t = int(st)
                    d = int(seg.get('d', 0))
                    rep = int(seg.get('r', 0))
                    for _ in range(rep + 1):
                        ts.append(t)
                        t = t + d if t is not None else None
            for idx, t in enumerate(ts):
                seg_url = media_url.replace('$Time$', str(t)).replace('$Number$', str(idx + 1))
                name = f'seg-{t}.m4v' if kind == 'video' else f'seg-{t}.m4a'
                fpath = out_dir / name
                if fpath.exists() and fpath.stat().st_size > 0:
                    files.append(fpath)
                    continue
                try:
                    fpath.write_bytes(self._fetch(seg_url, binary=True))
                    files.append(fpath)
                    if idx % 50 == 0:
                        print(f'[dash] {kind} segment {idx}/{len(ts)}')
                except Exception as exc:
                    print(f'[dash] {kind} segment {t} failed: {exc}')
                    break
            return files

        video_files = download_adaptation(video_ad, 'video')
        audio_files = download_adaptation(audio_ad, 'audio') if audio_ad is not None else []
        if not video_files:
            return None

        def concat(files, out):
            out = self.output_dir / out
            with open(out, 'wb') as dst:
                for f in files:
                    if f.exists():
                        dst.write(f.read_bytes())
            return out

        raw_video = concat(video_files, 'video.m4v')
        merged = self.output_dir / 'merged.mp4'
        if audio_files:
            raw_audio = concat(audio_files, 'audio.m4a')
            subprocess.run(
                ['/usr/bin/ffmpeg', '-y', '-loglevel', 'error', '-i', str(raw_video), '-i', str(raw_audio), '-c', 'copy', str(merged)],
                check=True,
            )
        else:
            shutil.copy(raw_video, merged)
        return merged


def fetch_live_info(cl, broadcast_id):
    try:
        return cl.media_get_livestream_info(broadcast_id)
    except Exception as exc:
        return {'error': str(exc)[:200]}


def collect_broadcasts(cl, uid):
    try:
        feed = cl.private_request(f'feed/user/{uid}/story/')
    except Exception as exc:
        print(f'[probe] story feed failed for {uid}: {exc}')
        return []
    broadcasts = []
    active = feed.get('broadcast')
    if active:
        broadcasts.append(('active', active))
    for b in (feed.get('post_live_item') or {}).get('broadcasts') or []:
        broadcasts.append(('post_live', b))
    return broadcasts


def replay_dash_url(broadcast):
    for key in ('dash_abr_playback_url', 'dash_playback_url', 'dash_manifest'):
        value = broadcast.get(key)
        if value:
            return str(value).replace('\\u0026', '&')
    return None


def is_rate_limited(exc):
    response = getattr(exc, 'response', None)
    status = getattr(response, 'status_code', None)
    if status == 429:
        return True
    return '429' in str(exc)


def resolve_uid_cached(cl, handle):
    uids = {}
    if os.path.exists(UID_CACHE):
        try:
            uids = json.loads(open(UID_CACHE, encoding='utf-8').read())
        except Exception:
            uids = {}
    if handle in uids:
        return str(uids[handle])
    try:
        # Private API only (i.instagram.com/api/v1) — skips instagrapi's fallback
        # to the 429-prone public web profile endpoint entirely.
        uid = str(cl.user_info_by_username_v1(handle).pk)
    except Exception as exc:
        if is_rate_limited(exc):
            raise
        print(f'[probe] username resolution failed for {handle}: {exc}')
        return None
    uids[handle] = uid
    Path(UID_CACHE).write_text(json.dumps(uids), encoding='utf-8')
    return uid


def collect_timeline_replay_candidates(cl, uid):
    """IG no longer surfaces live replays through the story feed post_live_item;
    replays are published as long timeline videos. Return (pk, duration, taken_at)
    for videos posted today that look like replays."""
    import datetime as _dt
    candidates = []
    try:
        medias = cl.user_medias(uid, amount=20)
    except Exception as exc:
        print(f'[probe] timeline fetch failed for {uid}: {exc}')
        return candidates
    jst_now = _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=9)))
    today_start = _dt.datetime(jst_now.year, jst_now.month, jst_now.day, tzinfo=jst_now.tzinfo)
    for m in medias:
        if m.media_type != 2:
            continue
        taken = m.taken_at
        if taken.tzinfo is None:
            taken = taken.replace(tzinfo=_dt.timezone.utc)
        taken_jst = taken.astimezone(_dt.timezone(_dt.timedelta(hours=9)))
        if taken_jst < today_start:
            break
        duration = float(getattr(m, 'video_duration', 0) or 0)
        if duration >= 300:
            candidates.append({'pk': str(m.pk), 'duration': duration, 'taken_at': str(taken_jst)})
    return candidates


def download_timeline_replay(cl, handle, out_dir, candidate):
    bid = candidate['pk']
    handle_dir = out_dir / handle / bid
    if (handle_dir / 'merged.mp4').exists():
        print(f'[probe] {handle} {bid}: replay already downloaded, skip')
        return
    handle_dir.mkdir(parents=True, exist_ok=True)
    (handle_dir / 'broadcast.json').write_text(
        json.dumps({**candidate, 'source': 'timeline_clip_replay'}, ensure_ascii=False, indent=2),
        encoding='utf-8',
    )
    print(f"[probe] {handle} {bid}: timeline replay duration={candidate['duration']:.0f}s taken={candidate['taken_at']}")
    try:
        media = cl.media_info(bid)
        url = str(media.video_url or '')
        if not url:
            print(f'[probe] {handle} {bid}: no video url available')
            (handle_dir / 'NO_DASH.txt').write_text('no video url\n', encoding='utf-8')
            return
        print(f'[probe] {handle} {bid}: downloading {url[:100]}')
        cookies = load_cookies(COOKIE_FILE)
        req = urllib.request.Request(url, headers={
            'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_0 like Mac OS X) AppleWebKit/605.1.15 Mobile/15E148 Safari/604.1',
            'cookie': '; '.join(f'{k}={v}' for k, v in cookies.items()),
        })
        outfile = handle_dir / 'merged.mp4'
        with urllib.request.urlopen(req, timeout=600) as r, open(outfile, 'wb') as fh:
            while True:
                chunk = r.read(1 << 20)
                if not chunk:
                    break
                fh.write(chunk)
        size = outfile.stat().st_size
        print(f'[probe] {handle} {bid}: merged.mp4 {size} bytes')
        (handle_dir / 'DONE.txt').write_text(f'merged={size}\n', encoding='utf-8')
    except Exception as exc:
        print(f'[probe] {handle} {bid}: replay download failed: {exc}')


def process_handle(cl, handle, out_dir):
    uid = resolve_uid_cached(cl, handle)
    if uid is None:
        return
    broadcasts = collect_broadcasts(cl, uid)
    if not broadcasts:
        print(f'[probe] {handle}: no broadcasts')
        for candidate in collect_timeline_replay_candidates(cl, uid):
            download_timeline_replay(cl, handle, out_dir, candidate)
        return
    for kind, b in broadcasts:
        bid = str(b.get('id', 'unknown'))
        status = b.get('broadcast_status', '?')
        published = b.get('published_time')
        handle_dir = out_dir / handle / bid
        if (handle_dir / 'merged.mp4').exists():
            print(f'[probe] {handle} {bid}: already downloaded, skip')
            continue
        handle_dir.mkdir(parents=True, exist_ok=True)
        (handle_dir / 'broadcast.json').write_text(json.dumps(b, ensure_ascii=False, indent=2), encoding='utf-8')
        print(f'[probe] {handle} {bid} kind={kind} status={status} published={published}')
        # "before" live info snapshot
        (handle_dir / 'live_info.before.json').write_text(json.dumps(fetch_live_info(cl, bid), ensure_ascii=False, indent=2), encoding='utf-8')
        mpd = replay_dash_url(b)
        if not mpd:
            info = json.loads((handle_dir / 'live_info.before.json').read_text())
            if isinstance(info, dict):
                mpd = replay_dash_url(info)
        if not mpd:
            print(f'[probe] {handle} {bid}: no dash URL available')
            (handle_dir / 'NO_DASH.txt').write_text('no dash url\n', encoding='utf-8')
            continue
        print(f'[probe] {handle} {bid}: dash={mpd[:120]}')
        merged = DashReplayDownloader(mpd, handle_dir).download()
        (handle_dir / 'live_info.after.json').write_text(json.dumps(fetch_live_info(cl, bid), ensure_ascii=False, indent=2), encoding='utf-8')
        if merged and merged.exists():
            size = merged.stat().st_size
            print(f'[probe] {handle} {bid}: merged.mp4 {size} bytes')
            (handle_dir / 'DONE.txt').write_text(f'merged={size}\n', encoding='utf-8')
        else:
            print(f'[probe] {handle} {bid}: download incomplete')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--handles', default=','.join(DEFAULT_HANDLES))
    parser.add_argument('--once', action='store_true')
    parser.add_argument('--interval', type=int, default=1800)
    parser.add_argument('--out-dir', default='/app/ig-probe/replays')
    args = parser.parse_args()

    handles = [h.strip() for h in args.handles.split(',') if h.strip()]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    import datetime as _dt
    consecutive_429 = 0
    while True:
        jst_now = _dt.datetime.now(_dt.timezone(_dt.timedelta(hours=9)))
        jst_hour = jst_now.hour
        is_weekend = jst_now.weekday() >= 5
        if not is_weekend and jst_hour < 12:
            print(f'[probe] pass skipped (JST hour {jst_hour} < 12 on weekday, no probing 00-12h)')
            if args.once:
                return
            time.sleep(1800)
            continue
        started = time.time()
        print(f'[probe] pass start {time.strftime("%F %T")} handles={handles}')
        pass_rate_limited = False
        try:
            cl = get_client()
            for handle in handles:
                try:
                    process_handle(cl, handle, out_dir)
                except Exception as exc:
                    if is_rate_limited(exc):
                        pass_rate_limited = True
                        print(f'[probe] 429 hit while processing {handle}: {exc}')
                    else:
                        print(f'[probe] handle error {handle}: {exc}')
        except Exception as exc:
            if is_rate_limited(exc):
                pass_rate_limited = True
                print(f'[probe] pass 429: {exc}')
            else:
                print(f'[probe] pass error: {exc}')
        if pass_rate_limited:
            consecutive_429 += 1
            # 429 backoff: stretch the pause, then keep doubling up to 60 min.
            backoff = min(3600, 1800 * (1 << min(consecutive_429 - 1, 1)))
            print(f'[probe] rate-limited; backing off {backoff}s (streak={consecutive_429})')
            if args.once:
                break
            time.sleep(backoff)
            continue
        consecutive_429 = 0
        if args.once:
            break
        elapsed = time.time() - started
        time.sleep(max(10, args.interval - elapsed))


if __name__ == '__main__':
    main()
