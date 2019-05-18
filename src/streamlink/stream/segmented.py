import concurrent.futures.thread
import logging
from concurrent import futures
from threading import Thread, Event
import hashlib

from .stream import StreamIO
from ..buffers import RingBuffer
from ..compat import queue

log = logging.getLogger(__name__)


class SegmentedStreamWorker(Thread):
    """The general worker thread.

    This thread is responsible for queueing up segments in the
    writer thread.
    """

    def __init__(self, reader, **kwargs):
        self.closed = False
        self.reader = reader
        self.writer = reader.writer
        self.stream = reader.stream
        self.session = reader.stream.session

        self._wait = None

        Thread.__init__(self, name="Thread-{0}".format(self.__class__.__name__))
        self.daemon = True

    def close(self):
        """Shuts down the thread."""
        if not self.closed:
            log.debug("Closing worker thread")

        self.closed = True
        if self._wait:
            self._wait.set()

    def wait(self, time):
        """Pauses the thread for a specified time.

        Returns False if interrupted by another thread and True if the
        time runs out normally.
        """
        self._wait = Event()
        return not self._wait.wait(time)

    def iter_segments(self):
        """The iterator that generates segments for the worker thread.

        Should be overridden by the inheriting class.
        """
        return
        yield

    def run(self):
        for segment in self.iter_segments():
            if self.closed:
                break
            self.writer.put(segment)

        # End of stream, tells the writer to exit
        self.writer.put(None)
        self.close()


class SegmentedStreamWriter(Thread):
    """The writer thread.

    This thread is responsible for fetching segments, processing them
    and finally writing the data to the buffer.
    """

    class MetaInfo(object):
        def __init__(self, path, buffering=None):
            self.file = open(path, "w", buffering=buffering)
            self.init()

        def init(self):
            """
            Extra inits
            """
            pass

        def close(self):
            self.file.close()

        def write(self, chunk):
            """
            Write chunk
            """
            pass

    class Info(MetaInfo):
        def init(self):
            self.segment_checksum = hashlib.md5()
            self.segment_size = 0

        def write(self, chunk):
            self.segment_checksum.update(chunk)
            self.segment_size += len(chunk)

        def segment_end(self, chunk_filename):
            self.file.write("{filename} {checksum} {length}\n".format(
                filename=chunk_filename,
                checksum="MD5=" + self.segment_checksum.hexdigest(),
                length=self.segment_size,
            ))
            self.init()

    class Checksum(MetaInfo):
        algorithms = ["md5", "sha256"]

        def init(self):
            self.checksums = [(algorithm, getattr(hashlib, algorithm)()) for algorithm in self.algorithms]

        def write(self, chunk):
            for algorithm, sumer in self.checksums:
                sumer.update(chunk)

        def close(self, filename=None):
            for algorithm, sumer in self.checksums:
                self.file.write("{algorithm} ({filename}) = {digest}\n".format(
                    algorithm=algorithm,
                    filename=filename,
                    digest=sumer.hexdigest(),
                ))
            super(SegmentedStreamWriter.Checksum, self).close()


    def __init__(self, reader, size=20, retries=None, threads=None, timeout=None, ignore_names=None, extra_info_path=None, checksum_path=None):
        self.closed = False
        self.reader = reader
        self.stream = reader.stream
        self.session = reader.stream.session

        if not retries:
            retries = self.session.options.get("stream-segment-attempts")

        if not threads:
            threads = self.session.options.get("stream-segment-threads")

        if not timeout:
            timeout = self.session.options.get("stream-segment-timeout")

        if not extra_info_path:
            extra_info_path = self.session.options.get("stream-segment-extra-info-path")

        if not checksum_path:
            checksum_path = self.session.options.get("stream-segment-checksum-path")

        self.retries = retries
        self.timeout = timeout
        self.ignore_names = ignore_names
        self.executor = futures.ThreadPoolExecutor(max_workers=threads)
        self.futures = queue.Queue(size)
        self.info = None
        if extra_info_path:
            self.info = SegmentedStreamWriter.Info(extra_info_path, buffering=1)
        self.checksum = None
        if checksum_path:
            self.checksum = SegmentedStreamWriter.Checksum(checksum_path, buffering=1)

        Thread.__init__(self, name="Thread-{0}".format(self.__class__.__name__))
        self.daemon = True

    def close(self):
        """Shuts down the thread."""
        if not self.closed:
            log.debug("Closing writer thread")

        self.closed = True
        self.reader.buffer.close()
        self.executor.shutdown(wait=False)
        if concurrent.futures.thread._threads_queues:
            concurrent.futures.thread._threads_queues.clear()

        if self.info:
            self.info.close()
            self.info = None

        if self.checksum:
            self.checksum.close()
            self.checksum = None

    def put(self, segment):
        """Adds a segment to the download pool and write queue."""
        if self.closed:
            return

        if segment is not None:
            future = self.executor.submit(self.fetch, segment,
                                          retries=self.retries)
        else:
            future = None

        self.queue(self.futures, (segment, future))

    def queue(self, queue_, value):
        """Puts a value into a queue but aborts if this thread is closed."""
        while not self.closed:
            try:
                queue_.put(value, block=True, timeout=1)
                return
            except queue.Full:
                continue

    def fetch(self, segment):
        """Fetches a segment.

        Should be overridden by the inheriting class.
        """
        pass

    def write(self, segment, result):
        """Writes a segment to the buffer.

        Should be overridden by the inheriting class.
        """
        pass

    def run(self):
        while not self.closed:
            try:
                segment, future = self.futures.get(block=True, timeout=0.5)
            except queue.Empty:
                continue

            # End of stream
            if future is None:
                break

            while not self.closed:
                try:
                    result = future.result(timeout=0.5)
                except futures.TimeoutError:
                    continue
                except futures.CancelledError:
                    break

                if result is not None:
                    self.write(segment, result)

                break

        self.close()


class SegmentedStreamReader(StreamIO):
    __worker__ = SegmentedStreamWorker
    __writer__ = SegmentedStreamWriter

    def __init__(self, stream, timeout=None):
        StreamIO.__init__(self)
        self.session = stream.session
        self.stream = stream

        if not timeout:
            timeout = self.session.options.get("stream-timeout")

        self.timeout = timeout

    def open(self):
        buffer_size = self.session.get_option("ringbuffer-size")
        self.buffer = RingBuffer(buffer_size)
        self.writer = self.__writer__(self)
        self.worker = self.__worker__(self)

        self.writer.start()
        self.worker.start()

    def close(self):
        self.worker.close()
        self.writer.close()
        self.buffer.close()

    def read(self, size):
        if not self.buffer:
            return b""

        return self.buffer.read(size, block=self.writer.is_alive(),
                                timeout=self.timeout)
