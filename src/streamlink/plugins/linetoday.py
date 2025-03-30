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
    r"https?://today\.line\.me/\w+/v2/article/",
))
class LineToday(Plugin):
    BROADCAST_STATUS = "LIVE"

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.session.set_option("hls-playlist-reload-time", "segment")

    @staticmethod
    def parse_simple_js_object(data):
        result = {}
        pattern = re.compile(r"\b(?P<key>\w+)\s*:\s*(?:(?P<dvalue>\d+)|\"(?P<svalue>[^\"]*)\")")
        return {
            m["key"]: int(m["dvalue"]) if m["dvalue"] is not None else m["svalue"] for m in re.finditer(pattern, data)
        }

    def _get_streams(self):
        script = self.session.http.get(
            self.url,
            schema=validate.Schema(
                validate.parse_html(),
                validate.xml_xpath_string("//script[contains(., 'shareProperties:{')][1]/text()"),
                str,
            ),
        )

        self.title = validate.Schema(
            validate.regex(re.compile(r"\bshareProperties\s*:\s*{(?P<shareProperties>[^}]*)}")),
            validate.get("shareProperties"),
            validate.transform(LineToday.parse_simple_js_object),
            validate.get("title"),
            str,
        ).validate(script)
        log.debug(f"title: {self.title}")

        media = validate.Schema(
            validate.regex(re.compile(r"\bmedia\s*:\s*{(?P<media>[^}]*)}")),
            validate.get("media"),
            validate.transform(LineToday.parse_simple_js_object),
            validate.union_get("type", "hash", "broadcastId"),
        ).validate(script)
        log.debug(f"media: {media}")

        if media[0] == "obs":
            log.debug(f"hash: {media[1]}")
            url = f"https://obs.line-scdn.net/{media[1]}/abr.m3u8"
            streams = HLSStream.parse_variant_playlist(
                self.session,
                url,
                headers={"Referer": self.url},
            )
        elif media[0] == "live":
            log.debug(f"broadcastId: {media[2]}")
            broadcast_status, self.title, hls_urls = self.session.http.get(
                f"https://today.line.me/webapi/glplive/broadcasts/{media[2]}",
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
            log.info(f"Unknown media type: {media[0]}")
            return

        return streams


__plugin__ = LineToday
