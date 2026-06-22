-- Singular test: fct_sales must never contain negative prices, quantities or
-- line revenue. Returns offending rows; the test passes when zero rows return.
select
    sales_order_line_id,
    unit_price,
    quantity,
    line_revenue
from {{ ref('fct_sales') }}
where unit_price < 0
   or quantity < 0
   or line_revenue < 0
