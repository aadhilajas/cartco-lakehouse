"""Gold layer business marts for CartCo lakehouse.

Contains business-facing analytical marts built on top of the unified
``silver.orders`` table:
    - ``daily_revenue``: Daily revenue aggregated by date and channel.
    - ``channel_performance``: Channel performance metrics (lifetime revenue, orders, cancellation/refund rates).
    - ``inventory_turnover``: Category sales and turnover by category and channel.
    - ``customer_360``: Customer order history, spend, and favorite category.
"""
