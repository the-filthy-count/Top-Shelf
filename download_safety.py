"""Protect unfinished downloads from import and release-folder cleanup."""
import os
from pathlib import Path


class PartialDownloadPending(RuntimeError):
    pass


def require_complete_download(entry):
    entry = Path(entry)
    if entry.name.lower().endswith('.part'):
        raise PartialDownloadPending(f'Waiting for partial download: {entry.name}')
    # A directly queued video must also respect unfinished siblings.
    root = entry if entry.is_dir() else entry.parent
    def failed(error):
        raise PartialDownloadPending(f'Cannot verify download directory: {error}')
    for directory, dirs, files in os.walk(root, onerror=failed, followlinks=False):
        if any(name.lower().endswith('.part') for name in files + dirs):
            raise PartialDownloadPending(f'Waiting for .part files in {root.name}')


def cleanup_download_directory(entry):
    """Never recursively delete a live tree: preserve partials appearing mid-import."""
    entry = Path(entry)
    require_complete_download(entry)
    for directory, dirs, files in os.walk(entry, topdown=False, followlinks=False):
        directory = Path(directory)
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
