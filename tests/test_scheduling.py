import ast
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import Mock
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from scheduling import INTERVAL_JOBS, interval_key, validate_schedule_settings

class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.settings={}
        self.scheduler=BackgroundScheduler(timezone='UTC')
        self.scheduler.start(paused=True)
        self.ns=dict(scheduler=self.scheduler,datetime=datetime,timedelta=timedelta,CronTrigger=CronTrigger)
        self.db=SimpleNamespace(get_settings=lambda:self.settings)
        self.ns['db']=self.db
        for _,callback,*_ in INTERVAL_JOBS:
            if callback.startswith('db.'):setattr(self.db,callback[3:],lambda:None)
            else:self.ns[callback]=lambda:None
        for callback in ('run_retry_pipeline','sync_tpdb_favourites','run_favourites_scheduled_scan','run_library_phash3_scheduled_job','_iafd_trickle_tick'):
            self.ns[callback]=lambda:None
        tree=ast.parse((Path(__file__).resolve().parents[1]/'main.py').read_text())
        names={'_apply_background_schedules','_schedule_clock_interval','_apply_retry_schedule','_apply_tpdb_sync_schedule','_apply_favourites_schedule','_apply_library_phash3_schedule','_apply_iafd_trickle_schedule'}
        exec(compile(ast.Module(body=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names],type_ignores=[]),'schedules','exec'),self.ns)
    def tearDown(self):self.scheduler.shutdown(wait=False)
    def test_defaults_updates_and_unrelated_saves_preserve_next_run(self):
        apply=self.ns['_apply_background_schedules'];apply()
        self.assertEqual(len(self.scheduler.get_jobs()),len(INTERVAL_JOBS))
        for job_id,_,_,unit,default,*_ in INTERVAL_JOBS:
            self.assertEqual(self.scheduler.get_job(job_id).trigger.interval.total_seconds(),default*{'seconds':1,'minutes':60,'hours':3600}[unit])
        next_run=self.scheduler.get_job('pending_check').next_run_time
        apply();self.assertEqual(self.scheduler.get_job('pending_check').next_run_time,next_run)
        self.settings[interval_key('pending_check')]='120';apply()
        self.assertEqual(self.scheduler.get_job('pending_check').trigger.interval.total_seconds(),120)
    def test_multiday_intervals_and_optional_jobs(self):
        self.settings.update(tpdb_sync_enabled='true',tpdb_sync_frequency_h='168',tpdb_sync_hour='2')
        self.ns['_apply_tpdb_sync_schedule']()
        trigger=self.scheduler.get_job('tpdb_sync').trigger
        first=trigger.get_next_fire_time(None,datetime(2026,9,12,tzinfo=timezone.utc))
        second=trigger.get_next_fire_time(first,first)
        self.assertEqual(second-first,timedelta(days=7))
        self.settings['tpdb_sync_enabled']='false';self.ns['_apply_tpdb_sync_schedule']()
        self.assertIsNone(self.scheduler.get_job('tpdb_sync'))
    def test_invalid_values_rejected_before_persistence(self):
        for key,value in [('schedule_pending_check_interval','0'),('retry_frequency_h','-1'),('tpdb_sync_hour','24'),('iafd_trickle_interval_minutes','1.5'),('favourites_scan_enabled','maybe')]:
            with self.assertRaises(ValueError):validate_schedule_settings({key:value})
        settings={'retry_frequency_h':'168','schedule_pending_check_interval':'120'}
        validate_schedule_settings(settings)
        self.assertEqual(settings['retry_frequency_h'],'168')
