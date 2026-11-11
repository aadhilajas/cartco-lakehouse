import json
from pathlib import Path
from types import SimpleNamespace

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


def snapshot_history():
    history=[SimpleNamespace(snapshot_id=n) for n in [1,2,3]]
    receipts={}
    for source in drill.runtime.SOURCE_ORDER:
        for operation in ['delete','append']:
            number=len(history)+1
            history.append(SimpleNamespace(snapshot_id=number,parent_snapshot_id=number-1,
                summary=SimpleNamespace(operation=SimpleNamespace(value=operation),
                    additional_properties={'cartco.batch_id':source+'-batch','cartco.merge.channel':source})))
        receipts[source]=dict(committed=True,batch_id=source+'-batch',state_snapshot_id=history[-1].snapshot_id)
    return history,receipts


def test_populated_partition_overwrites_have_two_snapshots_each():
    history,receipts=snapshot_history()
    evidence=drill.verify_state_snapshots([1,2,3],history,receipts,set(drill.runtime.SOURCE_ORDER))
    assert len(evidence)==6
    assert [x['operation'] for x in evidence]==['delete','append']*3


@pytest.mark.parametrize('corruption',['extra','missing','batch','parent','receipt'])
def test_overwrite_history_corruption_is_rejected(corruption):
    history,receipts=snapshot_history()
    if corruption=='extra':history.append(history[-1])
    elif corruption=='missing':history.pop()
    elif corruption=='batch':history[-1].summary.additional_properties['cartco.batch_id']='wrong'
    elif corruption=='parent':history[-1].parent_snapshot_id=1
    else:receipts['pos']['state_snapshot_id']=1
    with pytest.raises(RuntimeError):
        drill.verify_state_snapshots([1,2,3],history,receipts,set(drill.runtime.SOURCE_ORDER))
