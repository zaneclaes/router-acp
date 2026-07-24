# Orderflow Maintenance Task

Implement every `TODO` in:

- `src/orderflow/normalize.py`
- `src/orderflow/discounts.py`
- `src/orderflow/shipping.py`
- `src/orderflow/validation.py`
- `src/orderflow/summary.py`

Preserve the public function signatures. The required behavior is:

1. `normalize_sku`: trim, uppercase, replace each run of non-alphanumeric
   characters with one hyphen, strip edge hyphens, and reject an empty result.
2. `normalize_email`: trim and lowercase; require exactly one `@` with nonempty
   local and domain portions and at least one `.` in the domain.
3. `compute_discount`: use the greater of the customer-tier percentage and the
   coupon percentage, clamp coupons to 0-25%, and round percentage calculations
   to the nearest cent using `money.percent_of`.
4. `shipping_cost`: return zero for a domestic order whose post-discount
   subtotal is at least 10,000 cents. Otherwise charge the zone base plus the
   zone rate for each started 500g. Zero weight has zero weight blocks.
5. `validate_order`: return a sorted list of stable error codes. Validate the
   order id, email, zone, tier, coupon range, item presence, normalized SKU,
   positive quantity, positive unit price, and nonnegative weight. Prefix item
   errors with `item[<index>].`.
6. `summarize_order`: reject invalid orders with `ValueError` containing the
   comma-separated error codes. Otherwise return the exact keys asserted in
   `tests/test_summary.py`, using normalized identifiers and the existing tax
   helper.

Do not add dependencies or change tests.
