"""Application acceptance inside isolated services booted from restored volumes."""
import argparse
import json
from pathlib import Path
import sys
import time
from urllib.request import urlopen

sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from catalog_checkpoint import capture
from check_marquez_runtime import check
from pipeline.gold_release import read_gold_release


def wait_for(url):
    for attempt in range(90):
        try:
            with urlopen(url,timeout=5) as response:
                json.load(response)
            return
        except Exception:
            if attempt==89:
                raise
            time.sleep(2)


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkpoint',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    wait_for('http://iceberg-rest:8181/v1/config')
    before=json.loads(args.checkpoint.read_text(encoding='utf-8'))
    after=capture()
    if before!=after or not after['tables']:
        raise RuntimeError('Restored catalog content/schema/identity/snapshots differ from backup')
    root=Path('/opt/airflow/data/versioned')
    tables,manifest=read_gold_release(root/'gold')
    expected=dict(release_id=manifest['release_id'],state_snapshot_id=manifest['state_snapshot_id'],mart_rows=manifest['quality']['mart_rows'])
    candidates=[]
    for path in (root/'runs').glob('*/*/published.json'):
        if json.loads(path.read_text(encoding='utf-8'))['release_id']==manifest['release_id']:
            candidates.append(json.loads((path.parent/'run.json').read_text(encoding='utf-8'))['run_id'])
    if len(candidates)!=1:
        raise RuntimeError('Expected exactly one backed-up run for the current release')
    run_id=candidates[0]
    from airflow.models import DagRun,TaskInstance
    from airflow.utils.session import create_session
    with create_session() as session:
        run=session.query(DagRun).filter_by(dag_id='versioned_commerce_release',run_id=run_id).one()
        tasks=session.query(TaskInstance).filter_by(dag_id=run.dag_id,run_id=run_id).all()
        if run.state!='success' or len(tasks)!=6 or any(task.state!='success' for task in tasks):
            raise RuntimeError('Restored Airflow run/task records are incomplete')
        task_states={task.task_id:task.state for task in tasks}
    wait_for('http://marquez-api:5000/api/v1/namespaces')
    lineage,raw=check('http://marquez-api:5000',expected,run_id)
    report=dict(restored_service_boot_verified=True,catalog_tables=len(after['tables']),
                exact_catalog_checkpoint_preserved=True,gold_release=expected,
                restored_airflow_tasks=task_states,marquez=lineage,
                scope='Booted restored-copy MinIO/REST/Airflow Postgres/Marquez services with read-only runtime data; not fresh pipeline processing, in-flight worker crash recovery or offsite disaster readiness')
    args.output.with_suffix('.lineage.json').write_text(json.dumps(raw,indent=2)+'\n',encoding='utf-8')
    args.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(report,indent=2))


if __name__=='__main__':
    main()
