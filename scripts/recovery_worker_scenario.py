"""Synthetic fresh-delivery and post-commit task SIGKILL drill on isolated restored copies."""
import argparse
from collections import Counter
from datetime import date,datetime,timedelta,timezone
import hashlib
import importlib
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import time
from urllib.request import urlopen

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from scripts.catalog_checkpoint import capture
from pipeline import runtime
from pipeline.gold_release import read_gold_release
from pipeline.order_state import merge_order_versions
from pipeline.raw_batches import archive_csv
from pipeline.source_contracts import daily_contract
from pipeline.versioned_ingestion import canonical_events,STATE_TABLE

DAG='cartco_recovery_worker_drill'


def save(path,value):
    Path(path).write_text(json.dumps(value,sort_keys=True,indent=2)+'\n',encoding='utf-8')


def business_hash(table):
    rows=[{k:v for k,v in row.items() if k!='transform_timestamp'} for row in table.to_pylist()]
    digests=sorted(hashlib.sha256(json.dumps(row,sort_keys=True,default=str).encode()).digest() for row in rows)
    return hashlib.sha256(b''.join(digests)).hexdigest()


def old_gold():
    root=Path('/opt/airflow/data/versioned/gold')
    return {str(p.relative_to(root)):hashlib.sha256(p.read_bytes()).hexdigest() for p in root.rglob('*') if p.is_file()}


def verify_state_snapshots(original_ids,snapshots,receipts,existing_channels):
    """Pinned PyIceberg overwrite records delete+append for a populated partition."""
    ids=[s.snapshot_id for s in snapshots]
    if ids[:len(original_ids)]!=original_ids:
        raise RuntimeError('Original state snapshot history was changed')
    added=iter(snapshots[len(original_ids):])
    parent=original_ids[-1] if original_ids else None
    evidence=[]
    for source in runtime.SOURCE_ORDER:
        receipt=receipts[source]
        operations=['delete','append'] if source in existing_channels else ['append']
        for operation in operations:
            snapshot=next(added,None)
            if snapshot is None:
                raise RuntimeError('State overwrite snapshot history is incomplete')
            props=snapshot.summary.additional_properties
            if (snapshot.parent_snapshot_id!=parent or snapshot.summary.operation.value!=operation
                or props.get('cartco.batch_id')!=receipt['batch_id'] or props.get('cartco.merge.channel')!=source):
                raise RuntimeError('State snapshot ancestry/operation/batch evidence differs')
            evidence.append(dict(snapshot_id=snapshot.snapshot_id,source=source,operation=operation,batch_id=receipt['batch_id']))
            parent=snapshot.snapshot_id
        if receipt.get('committed') is not True or parent!=receipt['state_snapshot_id']:
            raise RuntimeError('Source receipt does not identify its completed overwrite')
    if next(added,None) is not None:
        raise RuntimeError('Unexpected additional state snapshot')
    return evidence


def prepare(output):
    ident=os.environ['CARTCO_DRILL_ID']
    work=Path('/opt/airflow/data/drill-runs')/ident
    inputs=Path('/opt/airflow/data/drill-deliveries')/ident
    if work.exists() or output.exists():
        raise RuntimeError('Use a fresh drill identity/volume copy')
    # Original frozen inputs determine a distinct next-day synthetic batch.
    days=[date.fromisoformat(json.loads(p.read_text())['batch_date']) for p in Path('/opt/airflow/data/versioned/runs').glob('*/*/run.json')]
    day=(max(days)+timedelta(days=1)).isoformat()
    before=capture()
    catalog=runtime.get_catalog()
    expected=catalog.load_table(STATE_TABLE).scan().to_arrow()
    source_hashes={}
    for source in runtime.SOURCE_ORDER:
        module=importlib.import_module('ingestion.generate_synthetic_'+source+'_data')
        module.N_ROWS=25
        frame=module.generate(day) if source=='pos' else module.generate(day,include_versions=True)
        target=inputs/day/(source+'.csv')
        target.parent.mkdir(parents=True,exist_ok=True)
        target.write_text(frame.to_csv(index=False),encoding='utf-8')
        contract=daily_contract(source,day)
        save(target.with_suffix('.csv.contract.json'),contract)
        source_hashes[source]=hashlib.sha256(target.read_bytes()).hexdigest()
        batch=archive_csv(target,Path('/opt/airflow/data/drill-expected')/ident,source=source,batch_date=day,contract=contract)
        expected=merge_order_versions(expected,canonical_events(batch),applied_at=datetime.now(timezone.utc)).rows
    save(output,dict(schema_version='cartco-worker-drill.v1',is_synthetic=True,dag_id=DAG,
        run_id='recovery_drill__'+ident,batch_date=day,work_root=str(work),input_root=str(inputs),
        expected_business_hash=business_hash(expected),expected_channels=dict(Counter(expected['channel'].to_pylist())),
        source_sha256=source_hashes,before=before,old_gold=old_gold()))
    print('Prepared 25 explicitly synthetic delivered rows per source for '+day)


def task_status(scenario):
    from airflow.models import DagRun,TaskInstance
    from airflow.utils.session import create_session
    with create_session() as session:
        run=session.query(DagRun).filter_by(dag_id=DAG,run_id=scenario['run_id']).one_or_none()
        tasks=session.query(TaskInstance).filter_by(dag_id=DAG,run_id=scenario['run_id']).all()
        states={t.task_id:dict(state=t.state,pid=t.pid,try_number=t.try_number) for t in tasks}
        return dict(state=run.state if run else 'missing',tasks=states,
            marker_ready=(Path(scenario['work_root'])/'worker-marker.json').is_file())


def trigger(scenario):
    from airflow.models import DagModel
    from airflow.utils.session import create_session
    for attempt in range(90):
        with create_session() as session:
            model=session.query(DagModel).filter_by(dag_id=DAG).one_or_none()
            ready=bool(model and model.is_active)
        if ready:
            break
        time.sleep(2)
    if not ready:
        raise RuntimeError('Acceptance DAG not registered by the scheduler')
    subprocess.run(['airflow','dags','unpause',DAG],check=True)
    subprocess.run(['airflow','dags','trigger',DAG,'--run-id',scenario['run_id'],'--conf',json.dumps({'batch_date':scenario['batch_date']})],check=True)


def crash(scenario,output):
    status=task_status(scenario)
    task=status['tasks'].get('ingest_pos',{})
    marker=Path(scenario['work_root'])/'worker-marker.json'
    evidence=json.loads(marker.read_text())
    if status['state']!='running' or task.get('state')!='running' or evidence['dag_id']!=DAG or evidence['run_id']!=scenario['run_id'] or evidence['task_id']!='ingest_pos':
        raise RuntimeError('Refusing signal: task identity/state is not the isolated acceptance task')
    pid=evidence['pid']
    if not isinstance(pid,int) or pid<=1 or pid==os.getpid() or task.get('pid')!=pid:
        raise RuntimeError('Refusing signal: task PID does not match Airflow')
    process=Path(f'/proc/{pid}/stat').read_text()
    if process[process.rfind(')')+2:].split()[19]!=evidence['process_start_time']:
        raise RuntimeError('Refusing signal: PID was reused')
    if (output.parent/'crash.json').exists():
        raise RuntimeError('This drill already signalled its one task')
    checkpoint=capture()
    # Preserve evidence before sending the signal. No other PID/container is targeted.
    save(output.parent/'pre-crash.json',checkpoint)
    save(output.parent/'crash.json',dict(pid=pid,signal='SIGKILL',run_id=scenario['run_id'],task=task,marker=evidence))
    os.kill(pid,signal.SIGKILL)
    print('SIGKILL sent to the verified isolated acceptance task after its POS commit')


def finish(scenario,output):
    from scripts.check_runtime_observability import inspect_run
    # Completion can run after restarting only the stopped data services.
    for attempt in range(60):
        try:
            with urlopen(os.environ.get('ICEBERG_CATALOG_URI','http://iceberg-rest:8181').rstrip('/')+'/v1/config',timeout=5) as response:
                json.load(response)
            break
        except OSError:
            if attempt==59:
                raise
            time.sleep(2)
    state=task_status(scenario)
    if state['state']!='success' or len(state['tasks'])!=6 or any(t['state']!='success' for t in state['tasks'].values()):
        raise RuntimeError('Not all six scheduler tasks recovered successfully')
    if not (output.parent/'crash.json').is_file():
        raise RuntimeError('No controlled crash evidence')
    killed=json.loads((output.parent/'crash.json').read_text())
    pos=state['tasks']['ingest_pos']
    if pos['try_number']<killed['task']['try_number']+2 or pos['pid']==killed['pid']:
        raise RuntimeError('Airflow did not record a new POS task attempt/process')
    after=capture()
    save(output.parent/'post-retry-catalog.json',after)
    pre=json.loads((output.parent/'pre-crash.json').read_text())
    if after!=pre:
        raise RuntimeError('Retry duplicated or changed a catalog commit after POS completion')
    initial=scenario['before']['tables']
    for name,before in initial.items():
        current=after['tables'][name]
        if current['uuid']!=before['uuid']:
            raise RuntimeError('Unexpected snapshot count or table identity: '+name)
        if name==STATE_TABLE:
            continue
        expected_delta=1 if name in {'bronze.'+s+'_order_events' for s in runtime.SOURCE_ORDER} else 0
        if len(current['retained_snapshots'])!=len(before['retained_snapshots'])+expected_delta:
            raise RuntimeError('Unexpected snapshot count: '+name)
        if not expected_delta and current!=before:
            raise RuntimeError('Unrelated table changed: '+name)
    state_table=runtime.get_catalog().load_table(STATE_TABLE)
    work=Path(scenario['work_root'])
    frozen_path,frozen=runtime._load(work,scenario['batch_date'],scenario['run_id'])
    receipts={source:json.loads(runtime._receipt(frozen_path,source,frozen).read_text()) for source in runtime.SOURCE_ORDER}
    old_channels=set(state_table.scan(snapshot_id=initial[STATE_TABLE]['current_snapshot_id']).to_arrow()['channel'].to_pylist())
    snapshot_evidence=verify_state_snapshots(initial[STATE_TABLE]['retained_snapshots'],state_table.metadata.snapshots,receipts,old_channels)
    table=state_table.scan().to_arrow()
    if business_hash(table)!=scenario['expected_business_hash'] or dict(Counter(table['channel'].to_pylist()))!=scenario['expected_channels']:
        raise RuntimeError('Fresh fixture business state differs from the independently prepared merge')
    if old_gold()!=scenario['old_gold']:
        raise RuntimeError('Original backed-up Gold release files changed')
    marts,manifest=read_gold_release(work/'gold')
    if len(marts)!=4 or manifest['state_snapshot_id']!=after['tables'][STATE_TABLE]['current_snapshot_id']:
        raise RuntimeError('Fresh no-inventory Gold publication is incomplete or unpinned')
    journal=inspect_run(work,scenario['batch_date'],scenario['run_id'])
    if journal['validation_errors'] or journal['evidence_errors'] or journal['incomplete_invocations'] or journal['terminal_alerts'] or not journal['all_stages_have_completed'] or not journal['alerts'].get('retry_scheduled'):
        raise RuntimeError('Missing retry alert or invalid runtime journal evidence: '+json.dumps(journal))
    save(output,dict(fresh_restored_scheduler_processing_verified=True,post_commit_task_crash_retry_verified=True,
        is_synthetic=True,batch_date=scenario['batch_date'],run_id=scenario['run_id'],tasks=state['tasks'],
        channels=scenario['expected_channels'],catalog_snapshots_unchanged_by_retry=True,state_snapshot_evidence=snapshot_evidence,
        original_gold_files_preserved=True,release_id=manifest['release_id'],gold_artifacts=sorted(marts),journal=journal,
        scope='One SIGKILL of isolated POS task after complete commit/receipt, then Airflow retry; not arbitrary mid-commit crash or whole-scheduler loss'))
    print(json.dumps(json.loads(output.read_text()),indent=2))


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action',choices=['prepare','trigger','status','crash','finish'])
    parser.add_argument('--scenario',type=Path,default=Path('/evidence/scenario.json'))
    parser.add_argument('--output',type=Path,default=Path('/evidence/report.json'))
    args=parser.parse_args()
    if args.action=='prepare':
        prepare(args.scenario)
        return
    scenario=json.loads(args.scenario.read_text())
    if scenario.get('schema_version')!='cartco-worker-drill.v1' or scenario.get('dag_id')!=DAG or not scenario.get('is_synthetic'):
        raise RuntimeError('Unsupported/non-fixture recovery scenario')
    if args.action=='trigger':trigger(scenario)
    elif args.action=='status':
        status=task_status(scenario);save(args.output.parent/'status.json',status);print(json.dumps(status))
    elif args.action=='crash':crash(scenario,args.output)
    else:finish(scenario,args.output)


if __name__=='__main__':
    main()
