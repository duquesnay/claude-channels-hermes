#!/usr/bin/env python3
"""
Spike B — test whether claude --channels can see an image when its base64
is inlined as a data URL in the text prompt.

Protocol: Unix socket ~/.hermes/run/hermes-channel.sock
  send: {"type":"prompt","request_id":"<uuid>","content":"<text>"}\n
  recv: {"type":"result","request_id":"<uuid>","content":"<reply>"}\n

Success criterion: response contains "red" (case-insensitive) without any
hint in the prompt beyond the image data.

Usage:
  python3 spike_inline_test.py [variant_number]
  - No arg: runs variant 1 only
  - variant_number: 1-6, runs that specific variant

Variants tested (one at a time to avoid rate-limiting the live session):
  1. Markdown image syntax:  ![alt](data:image/png;base64,XXX)
  2. Raw data URL on own line: data:image/png;base64,XXX
  3. HTML img-like angle bracket: <data:image/png;base64,XXX>
  4. Code block wrapped: ```\ndata:image/png;base64,XXX\n```
  5. Explicit prefix: "Image encoded as base64: data:image/png;base64,XXX"
  6. Plain base64 string (no URL prefix, just the b64 data)
"""

import socket
import json
import os
import base64
import sys
import time
import uuid

SOCK_PATH = os.path.expanduser("~/.hermes/run/hermes-channel.sock")
IMAGE_PATH = os.path.join(os.path.dirname(__file__), "test-red.png")
TIMEOUT = 60  # seconds — claude takes a few seconds to respond

QUESTION = "What color is the dominant pixel in this image? Respond with just the color name in English."


def load_image_b64() -> str:
    with open(IMAGE_PATH, "rb") as f:
        return base64.b64encode(f.read()).decode()


def build_prompt(variant: int, b64: str) -> str:
    data_url = f"data:image/png;base64,{b64}"
    if variant == 1:
        return f"{QUESTION}\n\n![image]({data_url})"
    elif variant == 2:
        return f"{QUESTION}\n\n{data_url}"
    elif variant == 3:
        return f"{QUESTION}\n\n<{data_url}>"
    elif variant == 4:
        return f"{QUESTION}\n\n```\n{data_url}\n```"
    elif variant == 5:
        return f"{QUESTION}\n\nImage encoded as base64: {data_url}"
    elif variant == 6:
        return f"{QUESTION}\n\n{b64}"
    else:
        raise ValueError(f"Unknown variant: {variant}")


def send_prompt(content: str) -> str:
    req_id = str(uuid.uuid4())
    msg = json.dumps({"type": "prompt", "request_id": req_id, "content": content}) + "\n"

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as s:
        s.connect(SOCK_PATH)
        s.settimeout(TIMEOUT)
        s.sendall(msg.encode())

        buf = b""
        deadline = time.time() + TIMEOUT
        while time.time() < deadline:
            try:
                chunk = s.recv(4096)
                if not chunk:
                    break
                buf += chunk
                if b"\n" in buf:
                    line = buf.split(b"\n")[0]
                    resp = json.loads(line.decode())
                    if resp.get("request_id") == req_id:
                        return resp.get("content", "")
            except socket.timeout:
                break

    raise TimeoutError(f"No response within {TIMEOUT}s")


def evaluate(variant: int, response: str) -> bool:
    low = response.lower()
    success = "red" in low
    label = "SUCCESS" if success else "FAIL"
    print(f"[Variant {variant}] {label}")
    print(f"  Response: {response[:200]!r}")
    return success


def main():
    variant = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    b64 = load_image_b64()
    print(f"Image: {IMAGE_PATH} ({len(b64)} b64 chars)")
    print(f"Testing variant {variant}...")
    print()

    prompt = build_prompt(variant, b64)
    print(f"Prompt length: {len(prompt)} chars")
    print(f"Sending to {SOCK_PATH}...")
    print()

    try:
        response = send_prompt(prompt)
        evaluate(variant, response)
    except Exception as e:
        print(f"[Variant {variant}] ERROR: {e}")


if __name__ == "__main__":
    main()
