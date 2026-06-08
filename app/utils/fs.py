import os
import shutil
import errno
import logging
import stat
from pathlib import Path

logger = logging.getLogger(__name__)

# Filed videos: clients often finish as 0700; shutil.move keeps that mode. Sidecars (.nfo, thumbs) are
# new files and follow process umask (often group-writable). Align video with matching .nfo when present.
LIBRARY_FILED_MODE_DEFAULT = 0o664


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


def safe_move(src: Path, dst: Path, emit_cb=None) -> None:
    """Move *src* to *dst*; never leave both after a successful import."""
    src = Path(src)
    dst = Path(dst)

    def _emit(msg):
        if emit_cb:
            emit_cb(msg)
        else:
            logger.info(msg)

    if not src.exists():
        raise FileNotFoundError(f"Source not found: {src}")

    try:
        if src.resolve() == dst.resolve():
            _ensure_library_filed_permissions(dst)
            return
    except OSError:
        pass

    dst.parent.mkdir(parents=True, exist_ok=True)

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
