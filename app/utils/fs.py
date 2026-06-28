import os
import shutil
import errno
import logging
import stat
from pathlib import Path

from app.utils.text import FS_FILENAME_MAX_BYTES, shorten_filename

logger = logging.getLogger(__name__)

# Filed videos: clients often finish as 0700; shutil.move keeps that mode. Sidecars (.nfo, thumbs) are
# new files and follow process umask (often group-writable). Align video with matching .nfo when present.
LIBRARY_FILED_MODE_DEFAULT = 0o664

# Optional owner/group applied to created library files and directories.
# Set by main.set_library_ownership() from the Directories settings. Left
# as None when unset so we don't fight the inherited filesystem owner.
_LIBRARY_OWNER_UID: int | None = None
_LIBRARY_OWNER_GID: int | None = None


def set_library_ownership(uid: int | None, gid: int | None) -> None:
    global _LIBRARY_OWNER_UID, _LIBRARY_OWNER_GID
    _LIBRARY_OWNER_UID = uid if (uid is not None and uid >= 0) else None
    _LIBRARY_OWNER_GID = gid if (gid is not None and gid >= 0) else None


def _apply_library_ownership(path: Path) -> None:
    if _LIBRARY_OWNER_UID is None and _LIBRARY_OWNER_GID is None:
        return
    try:
        uid = _LIBRARY_OWNER_UID if _LIBRARY_OWNER_UID is not None else -1
        gid = _LIBRARY_OWNER_GID if _LIBRARY_OWNER_GID is not None else -1
        os.chown(path, uid, gid)
    except (OSError, PermissionError):
        pass


def safe_mkdir(path: Path) -> None:
    """``mkdir(parents=True, exist_ok=True)`` but chowns each ancestor
    we actually create to the configured library owner. Without this,
    folders created while the app runs as root stay root-owned even
    after the files inside are chmodded to 0664."""
    path = Path(path)
    to_create: list[Path] = []
    cur = path
    while cur and not cur.exists():
        to_create.append(cur)
        if cur.parent == cur:
            break
        cur = cur.parent
    path.mkdir(parents=True, exist_ok=True)
    for d in reversed(to_create):
        _apply_library_ownership(d)


def _ensure_library_filed_permissions(path: Path) -> None:
    try:
        if not path.is_file():
            return
        mode = LIBRARY_FILED_MODE_DEFAULT
        nfo_same_stem = path.with_name(path.stem + ".nfo")
        if nfo_same_stem.is_file():
            try:
                mode = stat.S_IMODE(nfo_same_stem.stat().st_mode)
            except OSError:
                pass
        if mode == 0:
            mode = LIBRARY_FILED_MODE_DEFAULT
        os.chmod(path, mode)
    except OSError:
        pass
    _apply_library_ownership(path)


def _shorten_path_basename(p: Path) -> Path:
    """Return a copy of *p* with its final filename (or directory) name
    truncated to the kernel's per-component limit. UTF-8 safe."""
    name = p.name
    if len(name.encode("utf-8")) <= FS_FILENAME_MAX_BYTES:
        return p
    if "." in name and not name.startswith("."):
        stem, _, ext = name.rpartition(".")
        if len(ext) <= 8:  # Don't treat "2024.12.31.title" as having an ext.
            new = shorten_filename(stem, "." + ext)
            return p.with_name(new)
    return p.with_name(shorten_filename(name, ""))


def safe_move(src: Path, dst: Path, emit_cb=None) -> None:
    """Move *src* to *dst*; never leave both after a successful import."""
    src = Path(src)
    dst = Path(dst)

    def _emit(msg):
        if emit_cb:
            emit_cb(msg)
        else:
            logger.info(msg)

    # Cap each path component (in case the parent folder name is also
    # over the limit, e.g. movie folder "<long-jav-title> (2026)"). The
    # final basename is the most common offender; truncate that last so
    # the warning shows the right file.
    parts = list(dst.parts)
    capped = False
    for i, part in enumerate(parts):
        if len(part.encode("utf-8")) > FS_FILENAME_MAX_BYTES:
            new_part = _shorten_path_basename(Path(part)).name
            if new_part != part:
                parts[i] = new_part
                capped = True
    if capped:
        new_dst = Path(*parts)
        _emit(
            f"  safe_move: filename exceeded 255 bytes — truncating "
            f"{dst.name!r} -> {new_dst.name!r}"
        )
        dst = new_dst

    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")

    try:
        if src.resolve() == dst.resolve():
            _ensure_library_filed_permissions(dst)
            return
    except OSError:
        pass

    safe_mkdir(dst.parent)

    try:
        src_stat = src.stat()
        try:
            dst_dir_stat = dst.parent.stat()
            same_device = src_stat.st_dev == dst_dir_stat.st_dev
        except OSError:
            same_device = False

        if same_device:
            try:
                os.replace(str(src), str(dst))
                _ensure_library_filed_permissions(dst)
                return
            except OSError as e:
                if getattr(e, "errno", None) not in (errno.EXDEV, 18) and "cross-device" not in str(e).lower():
                    raise

        _emit(f"  safe_move: cross-device move {src.name} -> {dst.parent}")
        shutil.copyfile(str(src), str(dst))

        if dst.stat().st_size != src.stat().st_size:
            if dst.exists():
                dst.unlink()
            raise OSError(f"Verification failed: {dst} size does not match {src}")

        _ensure_library_filed_permissions(dst)

        try:
            src.unlink()
        except OSError as unlink_err:
            if dst.is_file() and src.is_file():
                _emit(
                    f"  safe_move: copied to {dst.name} but could not remove "
                    f"source {src} — delete manually to avoid a duplicate"
                )
                raise OSError(
                    f"Duplicate risk: copied to {dst} but source remains at {src}"
                ) from unlink_err
            raise

    except Exception as e:
        logger.error(f"safe_move failed: {e}")
        raise
