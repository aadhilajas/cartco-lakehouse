"""Read-only GE validation of a retained release and its exact Silver snapshot."""
import argparse
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from pipeline.gold_release import read_gold_release
from pipeline.output_quality import validate_outputs
from pipeline.runtime import get_catalog
from pipeline.versioned_ingestion import STATE_TABLE


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--gold-root',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    marts,manifest=read_gold_release(args.gold_root)
    state=get_catalog().load_table(STATE_TABLE)
    if str(state.metadata.table_uuid)!=manifest['state_table_uuid']:
        raise RuntimeError('Release refers to a different state table')
    report=validate_outputs(state.scan(snapshot_id=manifest['state_snapshot_id']).to_arrow(),marts)
    report.update(release_id=manifest['release_id'],state_snapshot_id=manifest['state_snapshot_id'],
        verification='Read-only retrospective check; existing release bytes and pointer unchanged')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(success=report['success'],suites=len(report['suites']),release_id=manifest['release_id'])))


if __name__=='__main__':main()
