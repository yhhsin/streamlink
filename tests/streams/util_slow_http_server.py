#!/usr/bin/env python

import sys
import time
import threading
from io import BytesIO
from http import HTTPStatus
import http.server
import urllib.parse


class SlowHTTPRequestHandler(http.server.SimpleHTTPRequestHandler):
    def send_head(self):
        parse_result = urllib.parse.urlparse(self.path)
        print("path: {}".format(parse_result.path))
        self.query = urllib.parse.parse_qs(parse_result.query)
        print("query: {}".format(self.query))

        self.interval = 0
        self.length = 0
        if self.query:
            query = self.query
            if "length" in query:
                self.length = int(query["length"][0])
            print("length: {}".format(self.length))
            if "interval" in query:
                self.interval = float(query["interval"][0])
                self.chunk_size = 1
                if "chunk_size" in query:
                    self.chunk_size = int(query["chunk_size"][0])
                print("interval: {}".format(self.interval))
                print("chunk_size: {}".format(self.chunk_size))

        if self.length > 0:
            content = "0" * self.length
        else:
            content = "Content: " + parse_result.path + "\n"
        bcontent = content.encode("utf-8")
        self.content = BytesIO(bcontent)
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-type", "text/plain")
        self.send_header("Content-Length", len(bcontent))
        self.end_headers()
        return self.content

    def do_GET(self):
        f = self.send_head()
        if f:
            try:
                if self.interval:
                    self.slow_copy(f, self.wfile, interval=self.interval, chunk_size=self.chunk_size)
                else:
                    self.copyfile(f, self.wfile)
            finally:
                f.close()

    def slow_copy(self, src, dst, interval=0, chunk_size=1):
        fsrc_read = src.read
        fdst_write = dst.write
        while True:
            buf = fsrc_read(chunk_size)
            if buf is None or len(buf) <= 0:
                break
            written = fdst_write(buf)
            if dst.closed or dst.fileno() < 0:
                break
            if self.server._BaseServer__shutdown_request:
                break
            if interval > 0:
                print("sleep...")
                time.sleep(interval)


class RunSlowHTTPServer:
    def __init__(self, host="127.0.0.1", port=5315, handler_class=SlowHTTPRequestHandler):
        self.host = host
        self.port = port
        self.handler_class = handler_class
        self.httpd = http.server.HTTPServer((self.host, self.port), self.handler_class)

    def __call__(self):
        return self.run()

    def run(self):
        with self.httpd:
            self.httpd.serve_forever()

    def run_threaded(self):
        thread = threading.Thread(target=self)
        thread.start()
        return thread

    def shutdown(self):
        self.httpd.shutdown()


def main():
    rs = RunSlowHTTPServer()
    server_thread = rs.run_threaded()
    while True:
        line = sys.stdin.readline()
        line = line.rstrip()
        if line == "q" or line == "quit":
            print("Exiting...")
            rs.shutdown()
            server_thread.join(timeout=3)
            break
    rs2 = RunSlowHTTPServer()
    server_thread2 = rs2.run_threaded()
    rs2.shutdown()
    server_thread2.join(timeout=3)

    return 0


if __name__ == "__main__":
    sys.exit(main())
