"""Fail-closed guards for explicitly requested library folder deletion.

This is permanent deletion, not the queue recovery executor. Callers must not
remove database records until this operation has succeeded.
"""
import os
import shutil
from pathlib import Path


def checked_folder(folder, roots):
    path = Path(folder).expanduser().absolute()
    resolved = path.resolve()
    if path != resolved:
        raise ValueError('Library deletion does not follow symlinks')
    bases = [Path(row['path']).expanduser().resolve() for row in roots if row.get('path')]
    if any(resolved == base or resolved in base.parents for base in bases):
        raise ValueError('Cannot delete a configured library root or its parent')
    owners = [base for base in bases if base in resolved.parents and base.is_dir()]
    if not owners:
        raise ValueError('Folder is not under an available configured library root')
    if not resolved.is_dir():
        raise ValueError('Library folder is missing or unavailable; records were retained')
    return resolved


def delete_folder(folder, roots):
    path = checked_folder(folder, roots)
    # Inspect the entire tree before deleting anything. Incomplete downloads
    # and links require attention rather than recursive deletion.
    def failed(error):
        raise error
    for parent, directories, files in os.walk(path, followlinks=False, onerror=failed):
        for name in directories + files:
            child = Path(parent) / name
            if child.is_symlink():
                raise ValueError('Library folder contains a symlink; deletion cancelled')
            if name.lower().endswith('.part'):
                raise ValueError('Library folder contains a partial download; deletion cancelled')
    # Revalidate after scanning; shutil's fd-based implementation also protects
    # against directory symlink substitution while descending the tree.
    checked_folder(path, roots)
    if not shutil.rmtree.avoids_symlink_attacks:
        raise RuntimeError('Safe recursive deletion is unavailable on this platform')
    shutil.rmtree(path)
