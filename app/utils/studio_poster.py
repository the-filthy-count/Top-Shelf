"""Procedural studio posters: dark, single-hue light behind the original logo."""
import colorsys
import os
from pathlib import Path
import tempfile
import threading

import numpy as np
from PIL import Image, ImageFilter

_lock = threading.Lock()
POSTER_NAMES = {'poster.webp', 'poster.jpg', 'poster.jpeg', 'poster.png', 'folder.jpg'}


def has_poster(folder: Path) -> bool:
    return any(p.name.lower() in POSTER_NAMES for p in folder.iterdir())


def render_poster(logo: Image.Image, seed=None) -> Image.Image:
    rng = np.random.default_rng(seed)
    width, height = 250, 375
    y, x = np.mgrid[:height, :width]
    x = (x - width / 2) / width
    y = (y - height / 2) / height
    glow = np.exp(-((x / .27) ** 2 + (y / .29) ** 2) * 1.5)
    for _ in range(9):
        cx, cy = rng.uniform(-.18, .18, 2)
        sx, sy = rng.uniform(.10, .24, 2)
        glow += rng.uniform(.05, .20) * np.exp(-((x-cx)/sx)**2 - ((y-cy)/sy)**2)
    noise = Image.fromarray(rng.integers(0, 256, (12, 8), dtype=np.uint8))
    noise = noise.resize((width, height), Image.Resampling.BICUBIC).filter(ImageFilter.GaussianBlur(8))
    glow *= .65 + np.asarray(noise) / 255 * .55
    glow /= max(glow.max(), 1)
    color = np.array(colorsys.hsv_to_rgb(rng.random(), .68, 1.0))
    pixels = np.clip(7 + glow[..., None] * (color * 120 + 16), 0, 255).astype('uint8')
    poster = Image.fromarray(pixels).resize((1000, 1500), Image.Resampling.LANCZOS).convert('RGBA')
    mark = logo.convert('RGBA')
    bbox = mark.getchannel('A').getbbox()
    if bbox is None:
        raise ValueError('Studio logo is empty')
    mark = mark.crop(bbox)
    scale = min(760 / mark.width, 500 / mark.height)
    mark = mark.resize((max(1, round(mark.width*scale)), max(1, round(mark.height*scale))), Image.Resampling.LANCZOS)
    position = ((1000-mark.width)//2, (1500-mark.height)//2)
    shadow = Image.new('RGBA', poster.size)
    silhouette = Image.new('RGBA', mark.size, (0, 0, 0, 200))
    silhouette.putalpha(mark.getchannel('A').point(lambda a: int(a*.65)))
    shadow.paste(silhouette, (position[0], position[1]+10))
    poster = Image.alpha_composite(poster, shadow.filter(ImageFilter.GaussianBlur(18)))
    poster.alpha_composite(mark, position)
    return poster.convert('RGB')


def ensure_studio_poster(folder: Path, logo_path: Path, seed=None) -> bool:
    """Publish complete artwork without replacing an existing poster."""
    folder = Path(folder)
    with _lock:
        if has_poster(folder):
            return False
        with Image.open(logo_path) as logo:
            poster = render_poster(logo, seed)
        fd, name = tempfile.mkstemp(prefix='.studio-poster-', suffix='.webp', dir=folder)
        os.close(fd)
        temporary = Path(name)
        try:
            poster.save(temporary, 'WEBP', quality=92, method=6)
            if has_poster(folder):
                return False
            # Exclusive publication: never overwrite artwork from another writer.
            try:
                os.link(temporary, folder / 'poster.webp')
            except FileExistsError:
                return False
            return True
        finally:
            temporary.unlink(missing_ok=True)
