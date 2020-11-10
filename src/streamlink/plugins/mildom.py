# -*- coding: utf-8 -*-
import re
import datetime
import uuid
from pprint import pformat

from streamlink.plugin import Plugin
from streamlink.plugin.api import validate, useragents
from streamlink.stream import HLSStream



class Mildom(Plugin):
    _url_re = re.compile(r'''
        ^https?://(?:\w*\.)?mildom\.com
        /playback/(?P<user_id>\d+)\?v_id=(?P<v_id>\d+-\w+)
    ''', re.VERBOSE)

    _api_url = "https://cloudac.mildom.com/nonolive/videocontent/playback/getPlaybackDetail"

    @classmethod
    def can_handle_url(cls, url):
        return cls._url_re.match(url) is not None

    def _get_video_streams(self, video_link):
        for video in video_link:
            yield (video["definition"], HLSStream(self.session, video["url"]))

    def _get_streams(self):
        match = self._url_re.match(self.url)
        user_id = match.group("user_id")
        v_id = match.group("v_id")

        params = {
            "v_id": v_id,
            "timestamp": datetime.datetime.now(tz=datetime.timezone.utc).isoformat().replace("+00:00", "Z"),
            "__guest_id": "pc-gp-" + str(uuid.uuid4()),
            "__location": "Taiwan|Taipei City",
            "__country": "Taiwan",
            "__cluster": "aws_japan",
            "__platform": "web",
            "__la": "ja",
            "__pcv": "v2.7.11",
            "sfr": "pc",
            "accessToken": "",
        }
        self.logger.debug("params = {}".format(params))
        res = self.session.http.get(self._api_url, params=params)
        self.logger.debug("res = {}".format(res))

        if res.ok:
            json = self.session.http.json(res)
            self.logger.debug(pformat(json))
            video_link = json["body"]["playback"]["video_link"]
            streams = list(self._get_video_streams(video_link))
            self.logger.debug("streams = {}".format(streams))

        return streams




__plugin__ = Mildom

