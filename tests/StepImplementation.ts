import { Step } from 'gauge-ts';
import assert = require('assert');
import axios, { AxiosResponse } from 'axios';
import { Kafka, logLevel } from 'kafkajs';
import * as avro from 'avsc';
import { chromium } from 'playwright';

const api = process.env.ORDER_API_URL || 'http://localhost:18081';
const projection = process.env.ORDER_PROJECTION_URL || 'http://localhost:18082';
const ui = process.env.ORDER_UI_URL || 'http://localhost:18080';
const registry = process.env.SCHEMA_REGISTRY_URL || 'http://localhost:18001';
const brokers = (process.env.KAFKA_BROKERS || 'localhost:19092').split(',');

async function until<T>(action: () => Promise<T>, predicate: (value: T) => boolean, timeoutMs = 12000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  let value = await action();
  while (!predicate(value) && Date.now() < deadline) {
    await new Promise(resolve => setTimeout(resolve, 300));
    value = await action();
  }
  if (!predicate(value)) throw new Error('Timed out waiting for expected evidence');
  return value;
}

async function decode(value: Buffer): Promise<{ schemaId: number; decoded: any }> {
  if (value[0] !== 0) throw new Error('Expected Confluent Avro magic byte');
  const schemaId = value.readUInt32BE(1);
  const schemaResponse = await axios.get(`${registry}/schemas/ids/${schemaId}`);
  return { schemaId, decoded: avro.Type.forSchema(JSON.parse(schemaResponse.data.schema)).fromBuffer(value.subarray(5)) };
}

export default class StepImplementation {
  private response!: AxiosResponse<any>;
  private order: any;

  @Step('Customer <customer> submits a limit <side> order for <quantity> <instrument> at <price> EUR')
  public async submitOrder(customer: string, side: string, quantity: string, instrument: string, price: string) {
    this.response = await axios.post(`${api}/orders`, { customerId: customer, side, quantity: Number(quantity), instrument, limitPrice: Number(price) });
    this.order = this.response.data;
  }

  @Step('The order command API accepts the order with status <status>')
  public async orderAccepted(status: string) {
    assert.equal(this.response.status, 201);
    assert.equal(this.order.status, status);
  }

  @Step('An Avro order-created event exists for the order on topic <topic>')
  public async orderCreatedEvent(topic: string) {
    const kafka = new Kafka({ clientId: 'order-acceptance', brokers, logLevel: logLevel.NOTHING });
    const consumer = kafka.consumer({ groupId: `acceptance-${Date.now()}` });
    await consumer.connect();
    await consumer.subscribe({ topic, fromBeginning: true });
    try {
      await new Promise<void>((resolve, reject) => {
        const timer = setTimeout(() => reject(new Error(`No ${this.order.orderId} event on ${topic}`)), 12000);
        consumer.run({ eachMessage: async ({ message }) => {
          const event = await decode(message.value as Buffer);
          if (event.decoded.orderId === this.order.orderId) { clearTimeout(timer); resolve(); }
        }}).catch(reject);
      });
    } finally { await consumer.disconnect(); }
  }

  @Step('The order is partially executed through Kafka')
  public async partiallyExecuted() {
    await until(async () => (await axios.get(`${projection}/orders/${this.order.orderId}`, { validateStatus: () => true })).data, value => value.status === 'PARTIALLY_FILLED');
  }

  @Step('The order eventually reaches status <status>')
  public async reachesStatus(status: string) {
    this.order = await until(async () => (await axios.get(`${projection}/orders/${this.order.orderId}`, { validateStatus: () => true })).data, value => value.status === status);
  }

  @Step('The risk decision explains <reason>')
  public async riskReason(reason: string) {
    assert.ok(String(this.order.reason || '').toLowerCase().includes(reason.toLowerCase()), `Risk reason does not contain ${reason}`);
  }

  @Step('The order history shows the submitted order with status <status>')
  public async orderHistory(status: string) {
    const browser = await chromium.launch({ headless: true });
    const page = await browser.newPage();
    try {
      await page.goto(ui);
      const row = page.locator(`[data-order-id="${this.order.orderId}"]`);
      await row.waitFor({ state: 'visible', timeout: 12000 });
      assert.equal(await row.getAttribute('data-status'), status, `UI does not show ${status}`);
    } finally { await browser.close(); }
  }
}
