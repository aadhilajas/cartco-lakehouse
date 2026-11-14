from openlineage.client.run import Dataset
from openlineage.client.serde import Serde
from pipeline import column_lineage
from pipeline.gold_quality import build_marts
from pipeline.inventory import SCHEMA
from order_state_helpers import events,order

STATE=Dataset(namespace='iceberg://cartco-lakehouse',name='silver.order_state_v2')
CSV=Dataset(namespace='file:///frozen',name='inputs/pos.csv')


def fields(value):return Serde.to_dict(value)['fields']


def test_source_normalization_tracks_native_fields_and_prior_state():
    result=fields(column_lineage.silver('pos',CSV,STATE))
    assert {'store_id','terminal_id','transaction_id'} <= {f['field'] for f in result['order_id']['inputFields'] if f['name']==CSV.name}
    assert {'quantity','unit_price'} <= {f['field'] for f in result['order_total']['inputFields'] if f['name']==CSV.name}
    assert any(f['name']==STATE.name for f in result['order_total']['inputFields'])
    assert 'status' in {f['field'] for f in result['order_status']['inputFields']}


def test_every_commerce_output_has_derivation_and_sales_filters():
    marts,_=build_marts(events(order()))
    for name,table in marts.items():
        result=fields(column_lineage.gold(name,table.column_names,STATE,{}))
        assert set(result)==set(table.column_names)
        for column,item in result.items():
            assert item['transformationDescription']
            if column not in {'currency','avg_fulfillment_days'}:
                assert {'order_status','is_deleted'} <= {f['field'] for f in item['inputFields']}


def test_inventory_turnover_uses_stock_cost_and_order_evidence():
    inputs={name:Dataset(namespace='file:///inventory',name=name) for name in ['scope.csv','products.csv','allocations.csv','snapshots.csv','manifest.json']}
    result=fields(column_lineage.gold('inventory_turnover',SCHEMA.names,STATE,inputs))
    assert set(result)==set(SCHEMA.names)
    dependencies={(x['name'],x['field']) for x in result['turnover']['inputFields']}
    assert ('snapshots.csv','inventory_value') in dependencies
    assert ('allocations.csv','cogs') in dependencies
    assert ('scope.csv','unit_cost') in dependencies
    assert (STATE.name,'quantity') in dependencies
    assert not any(field=='unit_price' for _,field in dependencies)
