"""
$description LINE Today
$url today.line.me
$type live
"""

import logging
import re
from urllib.parse import parse_qsl, urlparse

from streamlink.plugin import Plugin, pluginmatcher
from streamlink.plugin.api import validate
from streamlink.stream.hls import HLSStream


log = logging.getLogger(__name__)


@pluginmatcher(re.compile(
    r"https?://today\.line\.me/\w+/v3/article/(?P<article_id>[^?]+)",
))
class LineToday(Plugin):
    BROADCAST_STATUS = "LIVE"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session.set_option("hls-playlist-reload-time", "segment")
        self.article_id = self.match["article_id"]

    @staticmethod
    def parse_simple_js_object(data):
        result = {}
        pattern = re.compile(r"\b(?P<key>\w+)\s*:\s*(?:(?P<dvalue>\d+)|\"(?P<svalue>[^\"]*)\")")
        return {
            m["key"]: int(m["dvalue"]) if m["dvalue"] is not None else m["svalue"] for m in re.finditer(pattern, data)
        }

    def _get_streams(self):
        log.debug(f"article_id: {self.article_id}")

        script = self.session.http.get(
            self.url,
            schema=validate.Schema(
                validate.parse_html(),
                validate.xml_xpath_string("//script[@id='__NEXT_DATA__'][1]/text()"),
                str,
            ),
        )

        article_key = f"webapi/portal/page/setting/article?hash={self.article_id}"
        script_data = validate.Schema(
            validate.parse_json(),
            validate.get(("props", "pageProps", "fallback", article_key, "data")),
            dict,
        ).validate(script)

        self.title = validate.Schema(
            validate.get("title"),
            str,
        ).validate(script_data)
        log.debug(f"title: {self.title}")

        media = validate.Schema(
            validate.get("media", default=None),
        ).validate(script_data)
        if media is None:
            log.info("This stream is currently offline")
            return

        media_type = validate.Schema(
            validate.get("type"),
            str,
        ).validate(media)
        log.debug(f"media_type: {media_type}")

        if media_type == "obs":
            media_hash = validate.Schema(
                validate.get("hash"),
                str,
            ).validate(media)
            log.debug(f"hash: {media_hash}")

            streams = HLSStream.parse_variant_playlist(
                self.session,
                f"https://obs.line-scdn.net/{media_hash}/abr.m3u8",
                headers={"Referer": self.url},
            )
        elif media_type == "live":
            broadcast_id = validate.Schema(
                validate.get("broadcastId"),
                str,
            ).validate(media)
            log.debug(f"broadcast_id: {broadcast_id}")

            broadcast_status, self.title, hls_urls = self.session.http.get(
                f"https://today.line.me/webapi/glplive/broadcasts/{broadcast_id}",
                headers={
                    "Referer": self.url,
                },
                schema=validate.Schema(
                    validate.parse_json(),
                    {
                        "broadcastStatus": str,
                        "title": str,
                        "hlsUrls": dict,
                    },
                    validate.union_get(
                        "broadcastStatus",
                        "title",
                        "hlsUrls",
                    ),
                ),
            )
            if broadcast_status != self.BROADCAST_STATUS:
                log.info("This stream is currently offline")
                return
            log.debug(f"title: {self.title}")
            if len(hls_urls) == 1 and "abr" in hls_urls:
                streams = HLSStream.parse_variant_playlist(
                    self.session,
                    hls_urls["abr"],
                )
            else:
                streams = {
                    f"{label}p" if label.isdigit() else label: HLSStream(self.session, url) for label, url in hls_urls.items()
                }
        else:
            log.info(f"Unknown media type: {media_type}")
            return

        return streams


__plugin__ = LineToday
