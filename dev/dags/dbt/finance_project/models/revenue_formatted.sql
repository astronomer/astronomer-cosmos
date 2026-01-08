-- Testing cross-project macro usage
select
    customer_id,
    customer_name,
    {{ format_currency('total_revenue') }} as formatted_revenue
from {{ ref('revenue_by_customer') }}
