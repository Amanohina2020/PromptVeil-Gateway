"""HTTP/1.1 chunked transfer and server-sent-event framing."""

RESPONSE_HEAD = (b"HTTP/1.1 200 OK\r\nServer: promptveil-gateway\r\nContent-Type: text/event-stream; charset=utf-8\r\n"
                 b"Cache-Control: no-cache\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n")
TERMINAL_CHUNK = b"0\r\n\r\n"


def chunk_parts(data):
    """One HTTP chunk as three separate writes (chunk header, data, CRLF), i.e. three TLS records."""
    return [b"%x\r\n" % len(data), data, b"\r\n"]


class ChunkedSSEParser:
    """Incremental parser of an HTTP/1.1 response with a chunked body that carries SSE blocks.
    feed() returns every complete SSE block (event or comment, including its blank-line terminator)."""

    def __init__(self):
        self.buffer = b""
        self.head = None
        self.sse = b""
        self.state = "head"
        self.chunk_size = 0
        self.chunks = 0
        self.done = False

    def feed(self, data):
        self.buffer += data
        blocks = []
        while True:
            if self.state == "head":
                i = self.buffer.find(b"\r\n\r\n")
                if i < 0:
                    break
                self.head = self.buffer[:i + 4]
                self.buffer = self.buffer[i + 4:]
                self.state = "size"
            elif self.state == "size":
                i = self.buffer.find(b"\r\n")
                if i < 0:
                    break
                self.chunk_size = int(self.buffer[:i].split(b";")[0], 16)
                self.buffer = self.buffer[i + 2:]
                self.state = "trailer" if self.chunk_size == 0 else "data"
            elif self.state == "data":
                if len(self.buffer) < self.chunk_size + 2:
                    break
                data, crlf = self.buffer[:self.chunk_size], self.buffer[self.chunk_size:self.chunk_size + 2]
                if crlf != b"\r\n":
                    raise ValueError("chunk not terminated by CRLF")
                self.buffer = self.buffer[self.chunk_size + 2:]
                self.sse += data
                self.chunks += 1
                self.state = "size"
                while True:
                    j = self.sse.find(b"\n\n")
                    if j < 0:
                        break
                    blocks.append(self.sse[:j + 2])
                    self.sse = self.sse[j + 2:]
            elif self.state == "trailer":
                if self.buffer.startswith(b"\r\n"):
                    self.buffer = self.buffer[2:]
                    self.done = True
                    self.state = "end"
                break
            else:
                break
        return blocks
