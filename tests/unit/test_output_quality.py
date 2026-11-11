from decimal import Decimal
import json
import pyarrow as pa
import pytest
from order_state_helpers import events,order
from inventory_helpers import inputs,manifest,orders as inventory_orders
from pipeline.gold_quality import build_marts
from pipeline.output_quality import validate_outputs,OutputQualityError
from pipeline.inventory import build_turnover


def test_real_ge_suites_pass_and_are_json_serializable():
    rows=events(order())
    marts,_=build_marts(rows)
    report=validate_outputs(rows,marts)
    assert report['success'] and len(report['suites'])==6
    assert report['engine_version']=='0.18.19'
    json.dumps(report)


@pytest.mark.parametrize('corrupt',['negative','duplicate'])
def test_real_ge_rejects_bad_gold(corrupt):
    rows=events(order());marts,_=build_marts(rows)
    table=marts['daily_revenue']
    if corrupt=='negative':table=table.set_column(table.schema.get_field_index('total_revenue'),'total_revenue',pa.array([Decimal('-0.01')],pa.decimal128(18,2)))
    else:table=pa.concat_tables([table,table])
    marts['daily_revenue']=table
    with pytest.raises(OutputQualityError) as error:validate_outputs(rows,marts)
    assert not error.value.report['suites']['daily_revenue']['success']


def test_empty_valid_state_and_gold_are_allowed():
    rows=events();marts,_=build_marts(rows)
    assert validate_outputs(rows,marts)['success']


def test_inventory_missing_day_null_ratio_is_not_reported_as_zero():
    rows=inventory_orders();data=inputs();data['snapshots.csv'].pop()
    marts,_=build_marts(rows)
    marts['inventory_turnover'],_=build_turnover(rows,manifest(),data)
    report=validate_outputs(rows,marts)
    assert report['suites']['inventory_turnover']['success']
    assert marts['inventory_turnover']['turnover'].to_pylist()==[None]
