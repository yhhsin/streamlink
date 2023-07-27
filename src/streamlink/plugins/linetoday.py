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
#        self.session.set_option("hls-playlist-reload-time", "segment")

    def _get_streams(self):
        broadcast_id = self.session.http.get(
            self.url,
            schema=validate.Schema(
                validate.parse_html(),
                validate.xml_xpath_string("//script[contains(.,'broadcastId')][1]/text()"),
                str,
                validate.regex(re.compile(r'\bbroadcastId\s*:\s*"(?P<broadcastid>\w+)"')),
                validate.get("broadcastid"),
            ),
        )
        if not broadcast_id:
            return
        self.id = broadcast_id

        log.debug(f"Broadcast ID: {broadcast_id}")

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

        streams = {
            f"{label}p" if label.isdigit() else label: HLSStream(self.session, url) for label, url in hls_urls.items()
        }

        return streams


__plugin__ = LineToday
