import json
from pathlib import Path

import pyarrow as pa
import pytest

from scripts import recovery_worker_scenario as drill


def test_cli_help_starts_without_entering_a_scenario(monkeypatch,capsys):
    monkeypatch.setattr(drill.sys,'argv',['recovery_worker_scenario.py','--help'])
    with pytest.raises(SystemExit) as result:
        drill.main()
    assert result.value.code==0
    assert 'prepare' in capsys.readouterr().out


@pytest.mark.parametrize('change', ['run','task','state','pid'])
def test_signal_refused_for_mismatched_identity_or_pid(tmp_path,monkeypatch,change):
    scenario=dict(run_id='fixture-run',work_root=str(tmp_path))
    marker=dict(pid=123,process_start_time='unchanged',run_id='fixture-run',dag_id=drill.DAG,task_id='ingest_pos')
    status=dict(state='running',tasks={'ingest_pos':dict(state='running',pid=123,try_number=1)})
    if change=='run': marker['run_id']='different-run'
    elif change=='task': marker['task_id']='different-task'
    elif change=='state': status['tasks']['ingest_pos']['state']='success'
    else: status['tasks']['ingest_pos']['pid']=456
    (tmp_path/'worker-marker.json').write_text(json.dumps(marker),encoding='utf-8')
    monkeypatch.setattr(drill,'task_status',lambda _:status)
    def unexpected_signal(*args):
        pytest.fail('A mismatched process must never receive a signal')
    monkeypatch.setattr(drill.os,'kill',unexpected_signal)
    with pytest.raises(RuntimeError,match='Refusing signal'):
        drill.crash(scenario,tmp_path/'report.json')


def test_business_comparison_ignores_only_processing_timestamp():
    a=pa.Table.from_pylist([dict(order_id='x',quantity=2,transform_timestamp='first')])
    b=pa.Table.from_pylist([dict(order_id='x',quantity=2,transform_timestamp='later')])
    c=pa.Table.from_pylist([dict(order_id='x',quantity=3,transform_timestamp='later')])
    assert drill.business_hash(a)==drill.business_hash(b)
    assert drill.business_hash(a)!=drill.business_hash(c)


@pytest.mark.parametrize('reused_pid',[True,False])
def test_reused_pid_or_repeat_kill_is_refused(tmp_path,monkeypatch,reused_pid):
    scenario=dict(run_id='fixture-run',work_root=str(tmp_path))
    marker=dict(pid=123,process_start_time='old',run_id='fixture-run',dag_id=drill.DAG,task_id='ingest_pos')
    (tmp_path/'worker-marker.json').write_text(json.dumps(marker),encoding='utf-8')
    monkeypatch.setattr(drill,'task_status',lambda _:dict(state='running',tasks={'ingest_pos':dict(state='running',pid=123)}))
    original_read=Path.read_text
    def read(path,*args,**kwargs):
        if str(path).replace('\\','/').endswith('/proc/123/stat'):
            return '123 (fake) S '+'0 '*18+('new' if reused_pid else 'old')
        return original_read(path,*args,**kwargs)
    monkeypatch.setattr(Path,'read_text',read)
    if not reused_pid:
        (tmp_path/'crash.json').write_text('{}',encoding='utf-8')
    monkeypatch.setattr(drill.os,'kill',lambda *args:pytest.fail('Signal guard failed'))
    with pytest.raises(RuntimeError,match='PID was reused' if reused_pid else 'already signalled'):
        drill.crash(scenario,tmp_path/'report.json')
