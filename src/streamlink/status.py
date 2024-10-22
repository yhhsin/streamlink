import logging
import pkgutil
import warnings
from typing import Any, Callable, ClassVar, Dict, Iterator, Mapping, Optional, Tuple, Type

from streamlink import __version__, plugins
from streamlink.exceptions import NoPluginError, PluginError, StreamlinkDeprecationWarning
from streamlink.logger import StreamlinkLogger
from streamlink.options import Options
from streamlink.plugin.api.http_session import HTTPSession, TLSNoDHAdapter
from streamlink.plugin.plugin import NO_PRIORITY, Matcher, Plugin
from streamlink.utils.l10n import Localization
from streamlink.utils.module import load_module
from streamlink.utils.url import update_scheme


# Ensure that the Logger class returned is Streamslink's for using the API (for backwards compatibility)
logging.setLoggerClass(StreamlinkLogger)
log = logging.getLogger(__name__)


class Status:
    pass

class RichStatus(Status):
    def new_segment(
        self,
        id: str,
        total: Optional = None,
    ) -> Optional[int]:
        pass

    def update_segment(
        self,
        handle: int,
        total: Optional = None,
        completed: Optional = None,
        advance: Optional = None,
    ):
        pass

    def remove_segment(
        self,
        handle: int,
    ):
        pass


__all__ = ["Status", "RichStatus"]
