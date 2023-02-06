{% docs mrr %}
This model represents one record per month, per account (months have been filled
in to include any periods of inactivity).

This model classifies each month as one of: `new`, `reactivation`, `upgrade`,
`downgrade`, or `churn`.

{% enddocs %}


{% docs change_category %}

| Category       | Definition                                                                                              |
|----------------|---------------------------------------------------------------------------------------------------------|
| new            | The customer is a new customer that has not had a previous subscription                                 |
| churn          | Last month the customer paid for a subscription, but this month is not. A customer can churn many times |
| upgrade        | The customer has increased their usage and is now paying you more money per month                       |
| downgrade      | The customer has decreased their usage and is now paying you less money per month                       |
| reactivation   | A previously churned customer has started subscribing again                                             |
| (null)         | No change compared to last month                                                                        |


{% enddocs %}
