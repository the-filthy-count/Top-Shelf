"""Protect unfinished downloads from import and release-folder cleanup."""
import os
from pathlib import Path

VIDEO_EXTENSIONS = {".mp4", ".mkv", ".m4v", ".avi", ".wmv", ".mov", ".flv", ".webm", ".ts", ".m2ts", ".mpg", ".mpeg"}


class PartialDownloadPending(RuntimeError):
    pass


def require_complete_download(entry, *, shared_root=False):
    entry = Path(entry)
    if entry.name.lower().endswith('.part'):
        raise PartialDownloadPending(f'Waiting for partial download: {entry.name}')
    if shared_root and entry.is_file():
        # Loose files in staging are independent downloads, not one release.
        # Preserve the default recursive check for files inside release folders.
        related = {entry.name.lower() + '.part', entry.stem.lower() + '.part'}
        try:
            if any(child.name.lower() in related for child in entry.parent.iterdir()):
                raise PartialDownloadPending(f'Waiting for partial download: {entry.name}')
        except OSError as error:
            raise PartialDownloadPending(f'Cannot verify download directory: {error}') from error
        return
    # A directly queued video must also respect unfinished siblings.
    root = entry if entry.is_dir() else entry.parent
    def failed(error):
        raise PartialDownloadPending(f'Cannot verify download directory: {error}')
    for directory, dirs, files in os.walk(root, onerror=failed, followlinks=False):
        if any(name.lower().endswith('.part') for name in files + dirs):
            raise PartialDownloadPending(f'Waiting for .part files in {root.name}')


def cleanup_download_directory(entry):
    """Remove leftover junk, but defer cleanup while videos or partials remain."""
    entry = Path(entry)
    require_complete_download(entry)
    for directory, dirs, files in os.walk(entry, topdown=False, followlinks=False):
        directory = Path(directory)
        if any(Path(name).suffix.lower() in VIDEO_EXTENSIONS for name in files):
            raise PartialDownloadPending(f'Videos remain for import in {entry.name}')
        for name in files:
            if name.lower().endswith('.part'):
                continue
            try:
                (directory / name).unlink()
            except OSError:
                pass
        for name in dirs:
            if name.lower().endswith('.part'):
                continue
            child = directory / name
            try:
                if child.is_symlink():
                    child.unlink()
                else:
                    child.rmdir()
            except OSError:
                pass
    try:
        entry.rmdir()
    except OSError:
        pass


def require_complete_queue_video(video, settings):
    """Treat root files independently, but nested releases as a unit."""
    video = Path(video).resolve()
    for key in ('source_dir', 'vice_download_watch_dir'):
        raw = (settings.get(key) or '').strip()
        if not raw:
            continue
        root = Path(raw).expanduser().resolve()
        try:
            relative = video.relative_to(root)
        except ValueError:
            continue
        if len(relative.parts) == 1:
            require_complete_download(video, shared_root=True)
        else:
            require_complete_download(root / relative.parts[0])
        return
    require_complete_download(video)
