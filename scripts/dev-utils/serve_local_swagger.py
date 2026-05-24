from __future__ import annotations

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser(description="Serve the generated local Swagger/OpenAPI files over HTTP.")
    parser.add_argument("--host", default="127.0.0.1", help="Host interface to bind.")
    parser.add_argument("--port", type=int, default=8088, help="Port to serve the local Swagger UI on.")
    parser.add_argument(
        "--dir",
        default="local_swagger",
        help="Directory with football.openapi.json and index.html.",
    )
    args = parser.parse_args()

    directory = Path(args.dir).resolve()
    if not directory.exists():
        raise SystemExit(f"Directory does not exist: {directory}")

    handler = partial(SimpleHTTPRequestHandler, directory=str(directory))
    server = ThreadingHTTPServer((args.host, args.port), handler)
    print(f"Serving local Swagger from {directory}", flush=True)
    print(f"Open: http://{args.host}:{args.port}/", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
