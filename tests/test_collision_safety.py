import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch
from test_duplicate_filing_safety import load

class CollisionSafetyTests(unittest.TestCase):
    def test_failed_move_preserves_incumbent_and_hashes_actual_source(self):
        with tempfile.TemporaryDirectory() as tmp:
            src=Path(tmp)/'nested'/'same.mp4';src.parent.mkdir();src.write_bytes(b'new')
            dst=Path(tmp)/'same.mp4';dst.write_bytes(b'original')
            compute=Mock(return_value='hash');cached=Mock(side_effect=AssertionError('basename cache used'))
            env={'Path':Path,'compute_phash':compute,'db':SimpleNamespace(get_phash=cached,notification_add=Mock()),
                 '_phash_for_library_destination':lambda p:'hash','_phash_hamming_distance':lambda *a:0,
                 '_PHASH_SAME_SCENE_BITS':4,'_video_quality_score':lambda p:(2,3,2) if p==src else (1,3,1),
                 'emit':Mock(),'safe_move':Mock(side_effect=OSError('disk full'))}
            move=load('main.py','_quality_gate_move',env)
            with self.assertRaises(OSError):move(src,dst)
            self.assertEqual(dst.read_bytes(),b'original');self.assertTrue(src.exists())
            compute.assert_called_once_with(src);cached.assert_not_called()

    def test_missing_keepers_block_both_cleanup_paths(self):
        delete=Mock();env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{}),
             '_exact_queue_file_path':lambda *a:None,'_queue_delete_filename_sync':delete}
        resolve=load('main.py','_queue_duplicate_resolve_one_sync',env)
        self.assertFalse(resolve('missing',['only-copy'])['ok']);delete.assert_not_called()
        with tempfile.TemporaryDirectory() as tmp:
            env={'Path':Path,'_health_library_roots':lambda:[tmp],
                 '_health_assert_path_under_roots':lambda *a:None,'_duplicate_delete_one_library_video':delete}
            resolve=load('main.py','_duplicate_resolve_non_keepers',env)
            self.assertFalse(resolve(str(Path(tmp)/'missing'),['only-copy'],clear_pending_keeper_id=None)['ok'])
            delete.assert_not_called()

    def test_failed_unlink_is_error_for_scenes_and_movies(self):
        with tempfile.TemporaryDirectory() as tmp:
            video=Path(tmp)/'clip.mp4';video.touch()
            db=SimpleNamespace(update_file=Mock(),update_movie=Mock())
            discard=load('main.py','_queue_discard_source_after_library_phash',{'Path':Path,'db':db,'emit':Mock()})
            with patch.object(Path,'unlink',side_effect=PermissionError('denied')):
                for movie in (False,True):
                    result=discard(video,'clip.mp4','hash','duplicate',movie=movie)
                    self.assertEqual(result['status'],'error')
            self.assertEqual(db.update_file.call_args.kwargs['status'],'error')
            self.assertEqual(db.update_movie.call_args.kwargs['status'],'error')
            self.assertTrue(video.exists())

    def test_keeper_alias_is_not_deleted(self):
        with tempfile.TemporaryDirectory() as tmp:
            keeper=Path(tmp)/'keeper.mp4';keeper.touch();delete=Mock()
            env={'Path':Path,'db':SimpleNamespace(get_settings=lambda:{},media_file_signature=lambda p:'same'),
                 '_exact_queue_file_path':lambda name,settings:keeper,
                 '_queue_delete_filename_sync':delete}
            resolve=load('main.py','_queue_duplicate_resolve_one_sync',env)
            self.assertTrue(resolve('keeper.mp4',['./keeper.mp4'])['ok']);delete.assert_not_called()

    def test_unknown_fingerprint_preserves_both_videos(self):
        for source_hash, destination_hash in [('', ''), ('hash', ''), ('', 'hash')]:
            with self.subTest(source=source_hash,destination=destination_hash), tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);src=root/'incoming.mp4';dst=root/'existing.mp4';alternate=root/'existing - 1.mp4'
                src.write_bytes(b'incoming');dst.write_bytes(b'existing')
                env={'Path':Path,'compute_phash':lambda p:source_hash,
                     '_phash_for_library_destination':lambda p:destination_hash,
                     '_next_free_dest_slot':lambda p:alternate,'emit':Mock(),
                     'safe_move':lambda a,b:a.rename(b)}
                move=load('main.py','_quality_gate_move',env)
                self.assertEqual(move(src,dst),alternate)
                self.assertEqual(dst.read_bytes(),b'existing');self.assertEqual(alternate.read_bytes(),b'incoming')

    def test_batch_suppresses_cleanup_and_retry_enqueue(self):
        from contextvars import ContextVar
        flag=ContextVar('cleanup',default=False);enqueue=Mock();request=Mock()
        env={'_defer_client_cleanup':flag,'db':SimpleNamespace(download_cleanup_enqueue=enqueue),
             '_cleanup_downloads_status':request}
        cleanup=load('main.py','_remove_client_jobs_after_import',env)
        token=flag.set(True)
        try:cleanup('release.mp4',{'download_import_remove_client':'true'})
        finally:flag.reset(token)
        enqueue.assert_not_called();request.assert_not_called()

    def test_library_hash_uses_current_content(self):
        fresh=Mock(return_value='new-hash')
        cached=Mock(return_value={'phash_3':'old-hash'})
        fn=load('main.py','_phash_for_library_destination',{'Path':Path,'compute_phash':fresh,'db':SimpleNamespace(library_file_get_by_destination=cached)})
        self.assertEqual(fn(Path('video.mp4')),'new-hash')
        cached.assert_not_called()
        fresh.side_effect=OSError('unavailable')
        self.assertEqual(fn(Path('video.mp4')),'')

    def test_auto_duplicate_requires_verified_surviving_keeper(self):
        for case in ('missing','changed_content','changed_during_hash','changed_before_delete','valid','self'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                root=Path(tmp);incoming=root/'incoming.mp4';incoming.touch();keeper=root/'keeper.mp4'
                if case!='missing':keeper.touch()
                if case=='self':keeper=incoming
                discard=Mock(return_value={'status':'removed'})
                signatures=Mock(return_value='stable')
                if case=='changed_during_hash':signatures.side_effect=['before','after']
                if case=='changed_before_delete':signatures.side_effect=['stable','stable','changed']
                env={'Path':Path,'emit':Mock(),
                     'db':SimpleNamespace(library_files_active_matching_phash=lambda ph:[{'destination':str(keeper)}],media_file_signature=signatures),
                     '_phash_for_library_destination':lambda p:'different' if case=='changed_content' else 'hash',
                     '_health_dup_file_payload':lambda row:row,
                     '_queue_source_video_dup_compare_payload':lambda p:{'destination':str(p)},
                     '_duplicate_recommend':lambda rows:{'confidence':'high','keeper':rows[0]},
                     '_queue_discard_source_after_library_phash':discard}
                fn=load('main.py','_queue_resolve_phash_vs_indexed_library',env)
                result=fn(incoming,'incoming.mp4','hash')
                self.assertEqual(discard.call_count,1 if case=='valid' else 0)
                self.assertEqual(result,{'status':'removed'} if case=='valid' else None)
                self.assertTrue(incoming.exists())

    def test_library_delete_failure_keeps_all_records(self):
        with tempfile.TemporaryDirectory() as tmp:
            video=Path(tmp)/'scene.mp4';video.write_bytes(b'video')
            sidecar=video.with_suffix('.nfo');sidecar.write_text('metadata')
            db=SimpleNamespace(library_file_get_by_destination=lambda p:{'source_record_id':1},
                processed_file_delete_by_id=Mock(),library_file_delete_by_destination=Mock(),processed_files_delete_by_destination=Mock())
            env={'Path':Path,'db':db}
            load('main.py','_health_delete_video_and_sidecars',env)
            delete=load('main.py','_duplicate_delete_one_library_video',env)
            with patch.object(Path,'unlink',side_effect=PermissionError('denied')):
                with self.assertRaises(PermissionError):delete(str(video))
            for method in (db.processed_file_delete_by_id,db.library_file_delete_by_destination,db.processed_files_delete_by_destination):method.assert_not_called()
            self.assertTrue(video.exists());self.assertTrue(sidecar.exists())
            self.assertEqual(delete(str(video)),5)
            self.assertFalse(video.exists());self.assertFalse(sidecar.exists())
            db.library_file_delete_by_destination.assert_called_once_with(str(video))

    def test_post_filing_cleanup_verifies_both_current_files(self):
        for case in ('valid','old_hash_changed','replacement_hash_changed','old_changed','replacement_changed','replacement_missing'):
            with self.subTest(case=case),tempfile.TemporaryDirectory() as tmp:
                old=Path(tmp)/'old.mp4';new=Path(tmp)/'new.mp4';old.touch();new.touch()
                delete=Mock();counts={}
                def signature(p):
                    counts[p]=counts.get(p,0)+1
                    changed=(case=='old_changed' and p==old) or (case=='replacement_changed' and p==new)
                    return 'changed' if changed and counts[p]>=3 else 'stable'
                def fingerprint(p):
                    if (case=='old_hash_changed' and p==old) or (case=='replacement_hash_changed' and p==new):return 'different'
                    return 'hash'
                def recommend(rows):
                    if case=='replacement_missing':new.unlink()
                    return {'confidence':'high','keeper':rows[-1]}
                env={'Path':Path,'emit':Mock(),'db':SimpleNamespace(media_file_signature=signature,library_files_active_matching_phash=lambda h:[{'destination':str(old)}]),
                    '_phash_for_library_destination':fingerprint,'_queue_source_video_dup_compare_payload':lambda p:{'destination':str(p)},
                    '_health_dup_file_payload':lambda r:r,'_duplicate_recommend':recommend,'_duplicate_delete_one_library_video':delete}
                cleanup=load('main.py','_cleanup_superseded_library_copies',env)
                cleanup({'status':'filed','destination':str(new)},'hash')
                self.assertEqual(delete.call_count,1 if case=='valid' else 0)
