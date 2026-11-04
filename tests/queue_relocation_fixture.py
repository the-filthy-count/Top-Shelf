"""Exercise the real relocation effect with a unit test's queue database stub."""
from unittest.mock import patch
from app.core import queue_changes


def relocation_for(db):
    def relocate(filename,new_filename,moves):
        with patch.object(queue_changes,'db',db), patch.object(queue_changes,'execute',lambda fid,action,desired,apply:apply()):
            with patch.object(db,'file_identity',lambda name:{'id':'test'},create=True):
                return queue_changes.relocate(filename,new_filename,moves)
    return relocate
