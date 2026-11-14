"""Read-only check that Marquez exposes all dependencies declared by an event."""

# Support direct CLI execution and imports from any working directory.
import sys as _sys
from pathlib import Path as _Path
_SCRIPT_PATH = _Path(__file__).resolve()
_REPOSITORY_ROOT = next(
    (parent for parent in _SCRIPT_PATH.parents
     if (parent / "pipeline/source_identity.py").is_file()
     and (parent / "scripts/layout.json").is_file()),
    _SCRIPT_PATH.parent,  # Individually copied stdlib tools do not need a repo.
)
if str(_REPOSITORY_ROOT) not in _sys.path:
    _sys.path.insert(0, str(_REPOSITORY_ROOT))
import argparse
import json
from pathlib import Path
from urllib.parse import urlencode
from urllib.request import urlopen


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--event',type=Path,required=True)
    parser.add_argument('--url',default='http://localhost:5000')
    parser.add_argument('--output',type=Path,required=True)
    args=parser.parse_args()
    event=json.loads(args.event.read_text(encoding='utf-8'))
    results=[]
    for dataset in event['outputs']:
        fields=dataset.get('facets',{}).get('columnLineage',{}).get('fields',{})
        if not fields:raise RuntimeError('Event output has no declared column lineage')
        for field,definition in fields.items():
            if not definition['inputFields']:
                schema={f['name'] for f in dataset.get('facets',{}).get('schema',{}).get('fields',[])}
                if field not in schema or not definition['transformationDescription'].startswith('Constant'):
                    raise RuntimeError('An input-free field must be an explicitly declared schema constant')
                results.append(dict(dataset=dataset['name'],field=field,verified_input_edges=0,backend_edge_check='not_applicable_constant'))
                continue
            node=f"datasetField:{dataset['namespace']}:{dataset['name']}:{field}"
            with urlopen(args.url.rstrip('/')+'/api/v1/column-lineage?'+urlencode({'nodeId':node,'depth':1}),timeout=20) as response:
                nodes={entry['id']:entry for entry in json.load(response)['graph']}
            edges={(edge['origin'],edge['destination']) for entry in nodes.values() for edge in entry.get('inEdges',[])+entry.get('outEdges',[])}
            expected={(f"datasetField:{item['namespace']}:{item['name']}:{item['field']}",node) for item in definition['inputFields']}
            if node not in nodes or not expected<=edges:
                raise RuntimeError('Marquez column edges differ for '+dataset['name']+'.'+field)
            results.append(dict(dataset=dataset['name'],field=field,verified_input_edges=len(expected)))
    report=dict(column_graph_verified=True,run_id=event['run']['runId'],outputs=len(event['outputs']),fields=len(results),checks=results,
                scope='Current Marquez direct field edges for the supplied event; not exhaustive row-level/version provenance')
    args.output.parent.mkdir(parents=True,exist_ok=True)
    args.output.write_text(json.dumps(report,indent=2)+'\n',encoding='utf-8')
    print(json.dumps(dict(column_graph_verified=True,outputs=report['outputs'],fields=report['fields'])))


if __name__=='__main__':main()
