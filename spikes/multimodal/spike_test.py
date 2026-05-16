#!/usr/bin/env python3
"""
Spike test client for multimodal hermes-channel plugin.

Sends a prompt + image to the spike plugin socket and waits for reply.

Usage:
    python3 spike_test.py [socket_path] [image_path] [variant]

Defaults:
    socket_path = ~/.hermes/run/hermes-channel-spike.sock
    image_path  = test-red.png (64x64 red square)
    variant     = 1
"""

import socket
import json
import base64
import sys
import os
import time
from pathlib import Path

SPIKE_DIR = Path(__file__).parent
DEFAULT_SOCKET = os.path.expanduser("~/.hermes/run/hermes-channel-spike.sock")
DEFAULT_IMAGE = SPIKE_DIR / "test-red.png"

def send_and_receive(socket_path: str, image_path: str) -> dict:
    """Send multimodal IPC prompt and return parsed response."""
    request_id = f"spike-{int(time.time() * 1000)}"

    # Load image as base64
    with open(image_path, "rb") as f:
        image_bytes = f.read()
    image_b64 = base64.b64encode(image_bytes).decode("ascii")

    msg = {
        "type": "prompt",
        "request_id": request_id,
        "content": "What color is the shape in this image? Answer in one word.",
        "image_b64": image_b64,
        "image_mime": "image/png",
        "timeout_ms": 120000,
    }

    msg_line = json.dumps(msg) + "\n"

    print(f"[spike_test] Connecting to {socket_path}")
    print(f"[spike_test] request_id={request_id}")
    print(f"[spike_test] image={image_path} ({len(image_bytes)} bytes, {len(image_b64)} b64 chars)")
    print(f"[spike_test] Sending prompt: {msg['content']}")

    sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
    sock.connect(socket_path)

    sock.sendall(msg_line.encode("utf-8"))
    print(f"[spike_test] Sent. Waiting for response (timeout 120s)...")

    sock.settimeout(120.0)
    buf = ""
    while True:
        try:
            chunk = sock.recv(4096)
            if not chunk:
                print("[spike_test] Connection closed by server.")
                break
            buf += chunk.decode("utf-8")
            nl = buf.find("\n")
            if nl != -1:
                line = buf[:nl].strip()
                buf = buf[nl + 1:]
                if line:
                    response = json.loads(line)
                    return response
        except socket.timeout:
            print("[spike_test] Timeout waiting for response.")
            break

    return {"error": "no response received"}


def main():
    socket_path = sys.argv[1] if len(sys.argv) > 1 else DEFAULT_SOCKET
    image_path = sys.argv[2] if len(sys.argv) > 2 else str(DEFAULT_IMAGE)

    if not os.path.exists(socket_path):
        print(f"[spike_test] ERROR: socket not found at {socket_path}")
        print("[spike_test] Is the spike plugin session running?")
        sys.exit(1)

    if not os.path.exists(image_path):
        print(f"[spike_test] ERROR: image not found at {image_path}")
        sys.exit(1)

    response = send_and_receive(socket_path, image_path)

    print("\n=== RESPONSE ===")
    print(json.dumps(response, indent=2))
    print("================\n")

    content = response.get("content", "")
    if isinstance(content, str):
        content_lower = content.lower()
        if "rouge" in content_lower or "red" in content_lower:
            print("[spike_test] SUCCESS: Claude identified the red color!")
            sys.exit(0)
        elif "no image" in content_lower or "can't see" in content_lower or "cannot see" in content_lower:
            print("[spike_test] FAIL: Claude did not receive the image.")
            sys.exit(2)
        else:
            print(f"[spike_test] PARTIAL/AMBIGUOUS: Got response but no clear color identification.")
            print(f"[spike_test] Content: {content[:200]}")
            sys.exit(3)
    else:
        print(f"[spike_test] UNEXPECTED content type: {type(content)}")
        sys.exit(4)


if __name__ == "__main__":
    main()
