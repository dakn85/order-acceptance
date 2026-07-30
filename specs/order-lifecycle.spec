# ORD-101 — Customer can place and track a partially executed limit order

## Acceptance: a submitted limit order becomes visible across channels
tags: regression, orders, ORD-101

* Customer "alice" submits a limit "BUY" order for "10" "SAP" at "180" EUR
* The order command API accepts the order with status "NEW"
* An Avro order-created event exists for the order on topic "orders.created"
* The order is partially executed through Kafka
* The order history shows the submitted order with status "PARTIALLY_FILLED"

## Acceptance: a small order is filled completely
tags: regression, orders, execution, ORD-102

* Customer "pension-core" submits a limit "SELL" order for "4" "SIE" at "176.30" EUR
* The order command API accepts the order with status "NEW"
* The order eventually reaches status "FILLED"
* The order history shows the submitted order with status "FILLED"

## Acceptance: desk risk rejects excessive notional
tags: regression, orders, risk, ORD-103

* Customer "alpha-fund-eur" submits a limit "BUY" order for "2000" "ADS" at "218.35" EUR
* The order command API accepts the order with status "NEW"
* The order eventually reaches status "REJECTED"
* The risk decision explains "desk limit"
* The order history shows the submitted order with status "REJECTED"
