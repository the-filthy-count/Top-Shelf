"""Remote image downloads with transfer, time and decoded-size limits."""
import time
import requests
from io import BytesIO
from PIL import Image

MAX_BYTES = 25 * 1024 * 1024
MAX_PIXELS = 40_000_000

def download(url, timeout=35):
    if not str(url).lower().startswith(('http://', 'https://')):
        raise ValueError('Image URL must be http(s)')
    deadline = time.monotonic() + timeout
    with requests.get(url, stream=True, timeout=(5, min(10, timeout)),
                      headers={'User-Agent': 'Top-Shelf/1.0'}) as response:
        response.raise_for_status()
        if int(response.headers.get('Content-Length', '0')) > MAX_BYTES:
            raise ValueError('Image too large')
        data = bytearray()
        for chunk in response.iter_content(64 * 1024):
            if time.monotonic() > deadline:
                raise TimeoutError('Image transfer deadline exceeded')
            if len(data) + len(chunk) > MAX_BYTES:
                raise ValueError('Image too large')
            data.extend(chunk)
    result = bytes(data)
    # SVGs are rendered through the separate bounded renderer path.
    if b'<svg' not in result[:4096].lower():
        with Image.open(BytesIO(result)) as image:
            if image.width * image.height > MAX_PIXELS:
                raise ValueError('Image dimensions too large')
    return result
