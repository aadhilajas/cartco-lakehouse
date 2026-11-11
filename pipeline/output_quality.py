"""Pinned GE output suites complement exact decimal/business reconciliation."""
import great_expectations as gx
from pipeline.commerce_metrics import validated_orders

VERSION='cartco.ge.outputs.v1'
KEYS={'daily_revenue':['order_date','channel'],'channel_performance':['channel'],
      'customer_360':['customer_id','channel'],'category_unit_sales':['product_category','channel'],
      'inventory_turnover':['sku','location_id','period_start','period_end']}
NONNEGATIVE={'daily_revenue':['total_revenue','avg_order_value','order_count'],
    'channel_performance':['total_revenue','total_orders','placed_order_value','cancelled_order_value','refunded_order_value','pending_order_value'],
    'customer_360':['total_spend','total_orders'],'category_unit_sales':['units_sold'],
    'inventory_turnover':['cogs','inventory_value_days','observed_days','expected_days']}


class OutputQualityError(ValueError):
    def __init__(self,report):
        self.report=report
        super().__init__('Silver/Gold Great Expectations output gate failed')


def _suite(context,name,frame,checks):
    source=context.sources.add_pandas(name)
    asset=source.add_dataframe_asset(name=name+'_data')
    validator=context.get_validator(batch_request=asset.build_batch_request(dataframe=frame),create_expectation_suite_with_name=name+'_suite')
    validator.show_progress_bars=False
    results=[]
    for method,kwargs in checks:
        result=getattr(validator,method)(**kwargs,result_format='BASIC')
        results.append(dict(expectation=method,kwargs=kwargs,success=bool(result.success)))
    return dict(rows=len(frame),success=all(r['success'] for r in results),checks=results)


def _identity(keys):
    unique=('expect_column_values_to_be_unique',dict(column=keys[0])) if len(keys)==1 else ('expect_compound_columns_to_be_unique',dict(column_list=keys))
    return [('expect_column_values_to_not_be_null',dict(column=k)) for k in keys]+[unique]


def validate_outputs(orders,marts):
    if not set(KEYS)-{'inventory_turnover'} <= set(marts) or set(marts)-set(KEYS):
        raise ValueError('Output quality requires the declared Gold artifact set')
    context=gx.get_context(mode='ephemeral')
    suites={}
    suites['silver_state']=_suite(context,'silver_state',orders.to_pandas(),_identity(['channel','order_id'])+[
        ('expect_column_values_to_not_be_null',dict(column='source_updated_at')),
        ('expect_column_values_to_not_be_null',dict(column='is_deleted'))])
    active=validated_orders(orders).to_pandas()
    checks=_identity(['channel','order_id'])+[
        ('expect_column_values_to_not_be_null',dict(column=k)) for k in ['customer_id','order_date','product_category','quantity','unit_price','order_total','order_status','currency']]
    checks += [('expect_column_values_to_be_in_set',dict(column='channel',value_set=['shopify','amazon','pos'])),
        ('expect_column_values_to_be_in_set',dict(column='currency',value_set=['INR'])),
        ('expect_column_values_to_be_in_set',dict(column='order_status',value_set=['completed','cancelled','refunded','pending']))]
    checks += [('expect_column_values_to_be_between',dict(column=k,min_value=0)) for k in ['quantity','unit_price','order_total']]
    suites['silver_active']=_suite(context,'silver_active',active,checks)
    for name,table in sorted(marts.items()):
        checks=_identity(KEYS[name])
        for column in NONNEGATIVE[name]:
            checks += [('expect_column_values_to_not_be_null',dict(column=column)),
                       ('expect_column_values_to_be_between',dict(column=column,min_value=0))]
        if 'currency' in table.column_names:
            checks += [('expect_column_values_to_not_be_null',dict(column='currency')),
                       ('expect_column_values_to_be_in_set',dict(column='currency',value_set=['INR']))]
        if name=='channel_performance':
            checks += [('expect_column_values_to_be_between',dict(column=k,min_value=0,max_value=1)) for k in ['cancellation_rate','refund_rate']]
        if name=='inventory_turnover':
            checks += [('expect_column_values_to_be_in_set',dict(column='quality_status',value_set=['complete','incomplete_daily_coverage','zero_inventory_value']))]
        suites[name]=_suite(context,'gold_'+name,table.to_pandas(),checks)
    report=dict(suite_version=VERSION,engine='great_expectations',engine_version=gx.__version__,success=all(s['success'] for s in suites.values()),suites=suites)
    if not report['success']:
        raise OutputQualityError(report)
    return report
