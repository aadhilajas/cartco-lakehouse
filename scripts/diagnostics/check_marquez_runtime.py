"""Read-only Marquez indexing/graph acceptance for a specific runtime release."""

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
from urllib.parse import quote, urlencode
from urllib.request import urlopen


def check(base_url, expected, orchestration_run_id):
    def get(path):
        with urlopen(base_url.rstrip('/') + path, timeout=15) as response:
            return json.load(response)

    def require(condition, message):
        if not condition:
            raise RuntimeError(message)

    namespace = 'cartco-lakehouse'
    listing = get('/api/v1/namespaces/' + namespace + '/jobs?limit=100')
    jobs = {job['name']: job for job in listing['jobs']}
    reports = {}
    for stage in ('prepare', 'shopify', 'amazon', 'pos', 'inventory', 'publish'):
        name = 'versioned_commerce_release.' + stage
        require(name in jobs, f'Missing indexed job: {name}')
        run = jobs[name].get('latestRun') or {}
        require(run.get('state') == 'COMPLETED', f'Latest indexed run is not completed: {name}')
        facet = run.get('facets', {}).get('cartco_runtime', {})
        require(facet.get('orchestrationRunId') == orchestration_run_id and facet.get('stage') == stage,
                f'Indexed run identity differs: {name}')
        reports[stage] = dict(run_id=run['id'], state=run['state'],
                              input_versions=len(run.get('inputDatasetVersions', [])),
                              output_versions=len(run.get('outputDatasetVersions', [])))
    publication = jobs['versioned_commerce_release.publish']
    run = publication['latestRun']
    evidence = run['facets']['cartco_runtime']['evidence']
    require(evidence.get('releaseId') == expected['release_id'], 'Indexed release facet differs')
    require(evidence.get('receiptStateSnapshotId') == expected['state_snapshot_id'], 'Indexed snapshot facet differs')
    require(evidence.get('martRows') == expected['mart_rows'], 'Indexed mart counts differ')
    expected_outputs = {f"gold/releases/{expected['release_id']}/{name}.parquet" for name in expected['mart_rows']}
    outputs = run.get('outputDatasetVersions', [])
    require({value['datasetVersionId']['name'] for value in outputs} == expected_outputs,
            'Indexed output dataset versions differ from the release')
    require(all(value['datasetVersionId'].get('version') for value in outputs), 'Output version IDs missing')
    require(any(value['datasetVersionId']['name'] == 'silver.order_state_v2' for value in run.get('inputDatasetVersions', [])),
            'Indexed state input version missing')

    graph = get('/api/v1/lineage?' + urlencode({'nodeId': 'job:' + namespace + ':versioned_commerce_release.publish', 'depth': 4}))
    nodes = {node['id']: node for node in graph['graph']}
    edges = {(edge['origin'], edge['destination']) for node in nodes.values()
             for edge in node.get('inEdges', []) + node.get('outEdges', [])}
    state_id = 'dataset:iceberg://cartco-lakehouse:silver.order_state_v2'
    publish_id = 'job:cartco-lakehouse:versioned_commerce_release.publish'
    require((state_id, publish_id) in edges, 'Graph is missing state-to-publication edge')
    for source in ('shopify', 'amazon', 'pos'):
        require(('job:cartco-lakehouse:versioned_commerce_release.' + source, state_id) in edges,
                f'Graph is missing {source}-to-state edge')
    for name in expected_outputs:
        require((publish_id, 'dataset:file:///opt/airflow/data/versioned:' + name) in edges,
                f'Graph is missing publication output edge: {name}')
    fields = nodes.get(state_id, {}).get('data', {}).get('fields', [])
    require({'order_id', 'channel', 'source_updated_at', 'is_deleted'} <= {field['name'] for field in fields},
            'State schema fields are missing from the indexed graph')
    return dict(backend_graph_verified=True, release_id=expected['release_id'],
                state_snapshot_id=expected['state_snapshot_id'], orchestration_run_id=orchestration_run_id,
                jobs=reports, graph_nodes=len(nodes), graph_edges=len(edges), output_versions=len(outputs),
                scope='Completed indexed runs, runtime run facets, dataset versions, schema fields and graph edges; not durability/restart or dataset custom-facet acceptance'), dict(jobs=listing, graph=graph)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', default='http://localhost:5000')
    parser.add_argument('--expected', type=Path, required=True)
    parser.add_argument('--orchestration-run-id', required=True)
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    report, raw = check(args.url, json.loads(args.expected.read_text(encoding='utf-8-sig')), args.orchestration_run_id)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.with_suffix('.raw.json').write_text(json.dumps(raw, indent=2) + '\n', encoding='utf-8')
    args.output.write_text(json.dumps(report, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(report, indent=2))


if __name__ == '__main__':
    main()
