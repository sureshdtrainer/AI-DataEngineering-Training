1. order_id uniquely identifies an order.
2. Revenue is quantity multiplied by unit_price.
3. Only orders with status COMPLETED contribute to revenue.
4. CANCELLED and PENDING orders contribute zero revenue.
5. Customer summaries retain every customer, including customers with no
   completed orders.
6. Discounts, taxes, refunds, and currency conversion are outside this
   exercise's scope.
7. The order-level model must return one row per order.
8. The customer-level summary must return one row per customer.