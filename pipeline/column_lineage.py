"""Explicit field dependencies for implemented versioned transformations."""
from openlineage.client.facet import (ColumnLineageDatasetFacet,
    ColumnLineageDatasetFacetFieldsAdditional as Field,
    ColumnLineageDatasetFacetFieldsAdditionalInputFields as Input)


def facet(mapping):
    """mapping: output -> ([(dataset, input column)], precise transformation)."""
    return ColumnLineageDatasetFacet(fields={name:Field(inputFields=[Input(namespace=d.namespace,name=d.name,field=column) for d,column in dependencies],
        transformationDescription=description,transformationType='AGGREGATION' if description.startswith('GROUP') else 'CUSTOM')
        for name,(dependencies,description) in mapping.items()})


def silver(source,csv,state):
    columns=['order_id','customer_id','channel','order_date','product_id','product_category','quantity','unit_price','order_status','order_total','transform_timestamp','source_updated_at','is_deleted','currency']
    source_names={name:[name] for name in columns}
    source_names['product_id']=[{'shopify':'product_id','amazon':'asin','pos':'sku'}[source]]
    source_names['order_status']=['status']
    source_names['order_total']=['quantity','unit_price']
    source_names['transform_timestamp']=[]
    if source=='pos':
        source_names['order_id']=['store_id','terminal_id','transaction_id']
        source_names['customer_id']=['customer_id','order_id']
    return facet({name:([(csv,k) for k in inputs]+[(state,name),(state,'source_updated_at'),(state,'order_id'),(state,'channel'),(csv,'source_updated_at'),(csv,'is_deleted'),(csv,'order_id')],
        ('Derive exact decimal quantity * unit_price; ' if name=='order_total' else
         'Map source status to canonical vocabulary; ' if name=='order_status' else
         'Generate processing timestamp for changed rows; ' if name=='transform_timestamp' else
         'Convert timestamp to declared business date; ' if name=='order_date' else
         'Derive POS native UUID/customer or guest identity; ' if source=='pos' and name in {'order_id','customer_id'} else 'Canonical source field; ')
        +'merge by channel/order key and source version, retaining prior-state rows and tombstones') for name,inputs in source_names.items()})


def gold(name,columns,state,inventory):
    common=['order_status','is_deleted']
    maps={
      'daily_revenue':{'order_date':['order_date'],'channel':['channel'],'total_revenue':['order_total'],
        'order_count':['order_id'],'avg_order_value':['order_total','order_id']},
      'channel_performance':{'channel':['channel'],'total_orders':['order_id'],'total_revenue':['order_total'],
        'cancellation_rate':['order_status','order_id'],'refund_rate':['order_status','order_id'],
        'placed_order_value':['order_total'],'cancelled_order_value':['order_total','order_status'],
        'refunded_order_value':['order_total','order_status'],'pending_order_value':['order_total','order_status']},
      'customer_360':{'customer_id':['customer_id'],'channel':['channel'],'total_orders':['order_id'],'total_spend':['order_total'],
        'first_order_date':['order_date'],'last_order_date':['order_date'],'favorite_category':['product_category','quantity']},
      'category_unit_sales':{'product_category':['product_category'],'channel':['channel'],'units_sold':['quantity'],'reporting_period':['order_date']}}
    grouping={'daily_revenue':['order_date','channel'],'channel_performance':['channel'],
              'customer_360':['customer_id','channel'],'category_unit_sales':['product_category','channel']}
    result={}
    for column in columns:
        if name=='inventory_turnover':
            if column in {'sku','location_id'}: deps=[(inventory['scope.csv'],column)]
            elif column in {'period_start','period_end','currency','cost_basis','period_basis','is_synthetic'}:deps=[(inventory['manifest.json'],column)]
            else:
                deps=[(inventory['allocations.csv'],'cogs'),(inventory['snapshots.csv'],'inventory_value'),
                      (inventory['snapshots.csv'],'snapshot_date'),(inventory['manifest.json'],'period_start'),
                      (inventory['manifest.json'],'period_end'),(inventory['scope.csv'],'sku'),(inventory['scope.csv'],'location_id'),
                      (inventory['scope.csv'],'unit_cost'),(inventory['allocations.csv'],'quantity'),(inventory['allocations.csv'],'unit_cost'),
                      (inventory['products.csv'],'sku'),(inventory['products.csv'],'product_id')]
                deps += [(state,key) for key in ['order_id','channel','product_id','quantity','order_date','order_status','is_deleted']]
            result[column]=(deps,'Reconcile fixed-standard-cost allocations and scoped daily inventory; use explicit null ratios for incomplete or zero stock')
        elif column in {'currency','avg_fulfillment_days'}:
            result[column]=([], 'Constant INR contract' if column=='currency' else 'Constant null: fulfillment events unavailable')
        else:
            if column not in maps[name]:raise ValueError('Undocumented Gold output field: '+name+'.'+column)
            deps=list(dict.fromkeys(maps[name][column]+grouping[name]+common))
            result[column]=([(state,k) for k in deps],
                'GROUP canonical active orders by declared keys; completed-only sales/customer/category metrics, all-status channel counts and status value bridge; exact decimal money and deterministic category ties')
    return facet(result)
