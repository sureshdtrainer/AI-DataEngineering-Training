# Context Pack: Customer Revenue Transformation

## Objective
Create a customer-level retail revenue summary.

## Source datasets
- customers: customer_id, customer_name, region, signup_date
- orders: order_id, customer_id, product_id, order_date,
  quantity, unit_price, status

## Expected output grain
One row per customer.

## Join
LEFT JOIN customers to orders using customer_id.
Preserve customers without matching orders.

## Business rules
- Revenue = quantity * unit_price.
- Only COMPLETED orders contribute to revenue.
- CANCELLED and PENDING orders contribute zero revenue.
- Completed order count counts only COMPLETED orders.
- Customers without completed orders have zero count and revenue.
- Discounts, taxes, refunds, and currency conversion are out of scope.

## Engineering requirements
- Use explicit column names and aliases.
- Avoid multiplying rows through incorrect joins.
- Handle empty aggregates correctly.
- Explain assumptions and potential data quality risks.
- Provide validation queries or tests.
- Do not claim tests were executed unless they were run.

## Metadata references
- Schema: metadata/schemas.json
- Business rules: metadata/business_rules.md
- Mapping: metadata/mappings.json
- Lineage: metadata/lineage.json
