# Retail Business Rules

## Revenue
- Revenue = quantity * unit_price.
- Only COMPLETED orders contribute to revenue.
- CANCELLED and PENDING orders contribute zero revenue.
- Discounts, taxes, refunds, and currency conversion are out of scope.

## Customer reporting
- Customer summary must include all customers.
- Customers with no completed orders must have zero completed orders
  and zero completed revenue.
- Completed order count includes only COMPLETED orders.

## Data quality
- order_id must be unique.
- customer_id and product_id must be populated.
- Customer and product references must be valid.
- quantity must be greater than zero.
- unit_price must be non-negative.
- Invalid records must be reported rather than silently discarded.

## Modeling
- Identifiers are represented as strings.
- Order-level output has one row per order.
- Customer-level output has one row per customer.
