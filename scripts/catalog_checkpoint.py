"""Read-only REST catalog content checkpoint for quiesced demo-stack recreation."""
import argparse
import hashlib
import json
import os
from pathlib import Path


def capture():
    from pyiceberg.catalog import load_catalog
    catalog = load_catalog('checkpoint', type='rest', uri=os.environ.get('ICEBERG_CATALOG_URI','http://iceberg-rest:8181'), **{
        's3.endpoint':os.environ.get('S3_ENDPOINT','http://minio:9000'),
        's3.access-key-id':os.environ.get('AWS_ACCESS_KEY_ID','minioadmin'),
        's3.secret-access-key':os.environ.get('AWS_SECRET_ACCESS_KEY','minioadmin'),
        's3.region':os.environ.get('AWS_REGION','us-east-1')})
    tables = {}

    def walk(namespace=()):
        for child in catalog.list_namespaces(namespace):
            yield child
            yield from walk(child)

    for namespace in walk():
        for identifier in catalog.list_tables(namespace):
            table = catalog.load_table(identifier)
            rows = table.scan().to_arrow().to_pylist()
            digests = sorted(hashlib.sha256(json.dumps(row,sort_keys=True,default=str).encode()).digest() for row in rows)
            snapshot = table.current_snapshot()
            tables['.'.join(identifier)] = dict(rows=len(rows),row_multiset_sha256=hashlib.sha256(b''.join(digests)).hexdigest(),
                uuid=str(table.metadata.table_uuid),metadata_location=table.metadata_location,
                current_snapshot_id=snapshot.snapshot_id if snapshot else None,
                retained_snapshots=[s.snapshot_id for s in table.metadata.snapshots],
                schema=json.loads(table.schema().model_dump_json()),properties=table.properties)
    if not tables:
        raise RuntimeError('Refusing an empty catalog checkpoint')
    return dict(schema_version='cartco-catalog-checkpoint.v1',tables=tables)


def assert_airflow_quiescent():
    from airflow.models import DagModel, DagRun
    from airflow.utils.session import create_session
    with create_session() as session:
        if session.query(DagRun).filter(DagRun.state.in_(['queued','running'])).count():
            raise RuntimeError('Stop or finish all queued/running DAG runs before recreation')
        if session.query(DagModel).filter(DagModel.is_active.is_(True),DagModel.is_paused.is_(False)).count():
            raise RuntimeError('Pause all active DAGs before recreation')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    commands=parser.add_subparsers(dest='command',required=True)
    record=commands.add_parser('capture')
    record.add_argument('--output',type=Path,required=True)
    record.add_argument('--check-airflow-quiescent',action='store_true')
    compare=commands.add_parser('compare')
    compare.add_argument('--before',type=Path,required=True)
    compare.add_argument('--after',type=Path,required=True)
    args=parser.parse_args()
    if args.command=='capture':
        if args.check_airflow_quiescent:
            assert_airflow_quiescent()
        if args.output.exists():
            raise RuntimeError('Checkpoint output already exists; preserve it and choose a new name')
        report=capture()
        if args.check_airflow_quiescent:
            assert_airflow_quiescent()
        args.output.parent.mkdir(parents=True,exist_ok=True)
        with args.output.open('x',encoding='utf-8') as stream:
            json.dump(report,stream,indent=2)
        print(json.dumps(dict(tables=len(report['tables']),rows={name:value['rows'] for name,value in report['tables'].items()})))
    else:
        before=json.loads(args.before.read_text(encoding='utf-8'))
        after=json.loads(args.after.read_text(encoding='utf-8'))
        if before.get('schema_version')!='cartco-catalog-checkpoint.v1' or not before.get('tables') or before!=after:
            raise RuntimeError('Catalog content/identity/schema/snapshots changed across recreation')
        print(json.dumps(dict(catalog_checkpoints_match=True,tables=len(before['tables']),
            scope='Exact current row multiset, UUID, schema, properties, metadata location and retained snapshot IDs; caller must separately prove container recreation')))


if __name__=='__main__':
    main()
