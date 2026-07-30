# Securities Order Acceptance Contract

This is an independent Gauge repository for the application in `../securities-order-demo`. It is deliberately not a test fixture inside the application: a product platform should be able to import it from GitHub, resolve its Gauge specs and bindings, inject its evidence agent, and run it against a selected application revision.

## Coverage

The suite proves three business paths through multiple channels:

1. REST command accepts a customer limit order.
2. Kafka emits a Confluent-wire-format Avro `orders.created` event.
3. The asynchronous execution flow is reflected in the browser order history.
4. Small orders are fully filled while regular orders remain partially filled.
5. Excessive notional is rejected by the desk-risk rule and explained in the read model.

## Local execution

First start the independently versioned application:

```bash
cd ../securities-order-demo
docker compose up --build
```

Then, from this repository:

```bash
npm install
npx playwright install chromium
gauge run specs
```

For platform execution, activate the Node evidence agent before Gauge starts:

```bash
NODE_OPTIONS="--require @knippqai/adapter-sdk/register" gauge run specs
```

The test code itself uses ordinary Axios, KafkaJS and Playwright. No platform-specific client is imported.

## GitHub import exercise

Publish the directories separately, for example:

```text
github.com/knippqai/securities-order-demo
github.com/knippqai/securities-order-acceptance
```

The platform imports the acceptance repository, reads `manifest.json`, indexes `specs/` and `tests/`, then runs the selected revision against the demo application's compose environment. This tests repository discovery, stable story/suite metadata and generic Browser/REST/Kafka/Avro evidence without coupling acceptance assets to application code.
# order-acceptance-
