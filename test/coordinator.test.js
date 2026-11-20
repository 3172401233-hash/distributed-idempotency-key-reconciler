import test from 'node:test';
import assert from 'node:assert/strict';

import {
  IdempotencyCoordinator,
  Decision,
  CompleteStatus,
} from '../src/coordinator.js';
import { digestBody } from '../src/digest.js';

const T0 = 1_700_000_000_000;
const LEASE = 1_000;
const TTL = 60_000;

function makeCoordinator(options = {}) {
  return new IdempotencyCoordinator({
    leaseTtlMs: LEASE,
    ttlMs: TTL,
    ...options,
  });
}

function beginRequest(overrides = {}) {
  return {
    key: 'order-123',
    clientId: 'client-a',
    bodyDigest: digestBody({ sku: 'book', qty: 1 }),
    partition: 'p0',
    ownerId: 'worker-1',
    now: T0,
    ...overrides,
  };
}

test('相同键和相同摘要的并发重试：一个执行，其余等待，完成后复用结果', () => {
  const c = makeCoordinator();

  const first = c.begin(beginRequest());
  assert.equal(first.status, Decision.EXECUTE);
  assert.equal(first.holder, 'worker-1');
  assert.equal(first.epoch, 1);
  assert.equal(first.takeover, false);

  const retry = c.begin(beginRequest({ ownerId: 'worker-2', now: T0 + 10 }));
  assert.equal(retry.status, Decision.WAIT);
  assert.equal(retry.holder, 'worker-1');
  assert.equal(retry.retryAfterMs, LEASE - 10);
  assert.equal(retry.reusableResult, null);

  const done = c.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: first.epoch,
    result: { orderId: 'o-1' },
    now: T0 + 20,
  });
  assert.equal(done.status, CompleteStatus.COMMITTED);

  const late = c.begin(beginRequest({ ownerId: 'worker-3', now: T0 + 30 }));
  assert.equal(late.status, Decision.REPLAY);
  assert.deepEqual(JSON.parse(late.reusableResult.serialized), {
    orderId: 'o-1',
  });
  assert.equal(late.reusableResult.completedBy, 'worker-1');
  assert.equal(typeof late.reusableResult.resultDigest, 'string');
});

test('相同键不同请求体摘要返回冲突并给出依据', () => {
  const c = makeCoordinator();
  c.begin(beginRequest());

  const other = c.begin(
    beginRequest({
      bodyDigest: digestBody({ sku: 'book', qty: 2 }),
      ownerId: 'worker-2',
      now: T0 + 5,
    }),
  );
  assert.equal(other.status, Decision.CONFLICT);
  assert.equal(other.conflict.type, 'digest_mismatch');
  assert.equal(
    other.conflict.expectedDigest,
    digestBody({ sku: 'book', qty: 1 }),
  );
  assert.equal(
    other.conflict.receivedDigest,
    digestBody({ sku: 'book', qty: 2 }),
  );
  assert.equal(other.holder, 'worker-1');

  const record = c.inspect('order-123');
  assert.equal(record.state, 'executing');
  assert.equal(record.holder, 'worker-1');
});

test('执行实例失联后租约可被接管，epoch 递增', () => {
  const c = makeCoordinator();
  const first = c.begin(beginRequest());

  const duringLease = c.begin(
    beginRequest({ ownerId: 'worker-2', now: T0 + LEASE - 1 }),
  );
  assert.equal(duringLease.status, Decision.WAIT);

  const takeover = c.begin(
    beginRequest({ ownerId: 'worker-2', now: T0 + LEASE + 1 }),
  );
  assert.equal(takeover.status, Decision.EXECUTE);
  assert.equal(takeover.takeover, true);
  assert.equal(takeover.holder, 'worker-2');
  assert.ok(takeover.epoch > first.epoch);
});

test('旧实例晚到的结果不能覆盖新结果', () => {
  const c = makeCoordinator();
  const first = c.begin(beginRequest());

  const takeover = c.begin(
    beginRequest({ ownerId: 'worker-2', now: T0 + LEASE + 1 }),
  );
  assert.equal(takeover.status, Decision.EXECUTE);

  const stale = c.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: first.epoch,
    result: { orderId: 'stale-result' },
    now: T0 + LEASE + 2,
  });
  assert.equal(stale.status, CompleteStatus.STALE);

  const fresh = c.complete({
    key: 'order-123',
    ownerId: 'worker-2',
    epoch: takeover.epoch,
    result: { orderId: 'fresh-result' },
    now: T0 + LEASE + 3,
  });
  assert.equal(fresh.status, CompleteStatus.COMMITTED);

  const replay = c.begin(beginRequest({ now: T0 + LEASE + 4 }));
  assert.equal(replay.status, Decision.REPLAY);
  assert.deepEqual(JSON.parse(replay.reusableResult.serialized), {
    orderId: 'fresh-result',
  });
});

test('租约过期后旧持有者的提交被拒绝为 lease_expired', () => {
  const c = makeCoordinator();
  const first = c.begin(beginRequest());

  const late = c.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: first.epoch,
    result: { ok: true },
    now: T0 + LEASE + 1,
  });
  assert.equal(late.status, CompleteStatus.LEASE_EXPIRED);
  assert.equal(c.inspect('order-123').state, 'executing');
});

test('结果序列化失败标记记录失败，后续相同摘要请求被拒绝', () => {
  const c = makeCoordinator();
  const first = c.begin(beginRequest());

  const circular = {};
  circular.self = circular;
  const failed = c.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: first.epoch,
    result: circular,
    now: T0 + 10,
  });
  assert.equal(failed.status, CompleteStatus.SERIALIZATION_FAILED);
  assert.equal(failed.failure.type, 'result_serialization');

  const retry = c.begin(beginRequest({ ownerId: 'worker-2', now: T0 + 20 }));
  assert.equal(retry.status, Decision.REJECT);
  assert.equal(retry.conflict.type, 'execution_failed');
  assert.equal(retry.conflict.failure.type, 'result_serialization');

  const afterLease = c.begin(
    beginRequest({ ownerId: 'worker-2', now: T0 + LEASE + 1 }),
  );
  assert.equal(afterLease.status, Decision.EXECUTE);
  assert.equal(afterLease.takeover, true);
});

test('租约反复过期超过上限后拒绝接管', () => {
  const c = makeCoordinator({ maxTakeovers: 2 });
  c.begin(beginRequest());

  let now = T0 + LEASE + 1;
  const t1 = c.begin(beginRequest({ ownerId: 'worker-2', now }));
  assert.equal(t1.status, Decision.EXECUTE);

  now += LEASE + 1;
  const t2 = c.begin(beginRequest({ ownerId: 'worker-3', now }));
  assert.equal(t2.status, Decision.EXECUTE);

  now += LEASE + 1;
  const t3 = c.begin(beginRequest({ ownerId: 'worker-4', now }));
  assert.equal(t3.status, Decision.REJECT);
  assert.equal(t3.conflict.type, 'lease_flapping');
  assert.equal(t3.conflict.takeovers, 2);
});

test('跨分区迁移：租约活跃时冲突，租约过期后允许迁移接管', () => {
  const c = makeCoordinator();
  c.begin(beginRequest());

  const active = c.begin(
    beginRequest({ partition: 'p1', ownerId: 'worker-2', now: T0 + 5 }),
  );
  assert.equal(active.status, Decision.CONFLICT);
  assert.equal(active.conflict.type, 'partition_mismatch');
  assert.equal(active.conflict.expectedPartition, 'p0');
  assert.equal(active.conflict.receivedPartition, 'p1');

  const migrated = c.begin(
    beginRequest({
      partition: 'p1',
      ownerId: 'worker-2',
      now: T0 + LEASE + 1,
    }),
  );
  assert.equal(migrated.status, Decision.EXECUTE);
  assert.equal(migrated.takeover, true);
  assert.equal(c.inspect('order-123').partition, 'p1');
  assert.equal(c.inspect('order-123').migrations, 1);
});

test('清理只移除租约也不活跃的过期记录，与并发 begin 竞争安全', () => {
  const c = makeCoordinator();
  c.begin(beginRequest({ key: 'active-lease', now: T0 }));
  c.begin(beginRequest({ key: 'expired-lease', now: T0 - TTL - LEASE - 1 }));

  const done = c.begin(beginRequest({ key: 'done', now: T0 - TTL - 1 }));
  c.complete({
    key: 'done',
    ownerId: 'worker-1',
    epoch: done.epoch,
    result: { ok: 1 },
    now: T0 - TTL - 1,
    ttlMs: 1,
  });

  const sweep = c.cleanup(T0);
  assert.deepEqual(sweep.removed.sort(), ['done', 'expired-lease']);
  assert.equal(sweep.remaining, 1);
  assert.notEqual(c.inspect('active-lease'), null);

  const again = c.begin(
    beginRequest({ key: 'active-lease', ownerId: 'worker-2', now: T0 }),
  );
  assert.equal(again.status, Decision.WAIT);
  assert.equal(again.holder, 'worker-1');

  const fresh = c.begin(
    beginRequest({ key: 'expired-lease', ownerId: 'worker-9', now: T0 }),
  );
  assert.equal(fresh.status, Decision.EXECUTE);
  assert.equal(fresh.epoch, 1);
  assert.equal(fresh.takeover, false);
});

test('完成后的记录在 TTL 内可复用，过期清理后相同请求重新执行', () => {
  const c = makeCoordinator();
  const first = c.begin(beginRequest());
  c.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: first.epoch,
    result: { orderId: 'o-9' },
    now: T0 + 10,
  });

  const withinTtl = c.begin(beginRequest({ now: T0 + 10 + TTL - 1 }));
  assert.equal(withinTtl.status, Decision.REPLAY);

  const sweep = c.cleanup(T0 + 10 + TTL);
  assert.deepEqual(sweep.removed, ['order-123']);

  const restarted = c.begin(beginRequest({ now: T0 + 10 + TTL + 1 }));
  assert.equal(restarted.status, Decision.EXECUTE);
  assert.equal(restarted.takeover, false);
});

test('请求参数校验：缺少必填字段抛出 TypeError', () => {
  const c = makeCoordinator();
  assert.throws(() => c.begin(beginRequest({ key: '' })), TypeError);
  assert.throws(() => c.begin(beginRequest({ now: NaN })), TypeError);
  assert.throws(
    () => c.begin(beginRequest({ bodyDigest: undefined })),
    TypeError,
  );
});
