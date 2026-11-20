import { digestResult } from './digest.js';

export const Decision = Object.freeze({
  EXECUTE: 'execute',
  WAIT: 'wait',
  REPLAY: 'replay',
  CONFLICT: 'conflict',
  REJECT: 'reject',
});

export const CompleteStatus = Object.freeze({
  COMMITTED: 'committed',
  STALE: 'stale',
  LEASE_EXPIRED: 'lease_expired',
  UNKNOWN_KEY: 'unknown_key',
  SERIALIZATION_FAILED: 'serialization_failed',
});

const State = Object.freeze({
  EXECUTING: 'executing',
  COMPLETED: 'completed',
  FAILED: 'failed',
});

const DEFAULT_LEASE_TTL_MS = 30_000;
const DEFAULT_TTL_MS = 24 * 60 * 60 * 1000;
const DEFAULT_MAX_TAKEOVERS = 3;

function leaseActive(record, now) {
  return record.lease !== null && record.lease.expiresAt > now;
}

function holderOf(record) {
  return record.lease === null ? null : record.lease.owner;
}

export class IdempotencyCoordinator {
  constructor(options = {}) {
    this.records = new Map();
    this.defaultLeaseTtlMs = options.leaseTtlMs ?? DEFAULT_LEASE_TTL_MS;
    this.defaultTtlMs = options.ttlMs ?? DEFAULT_TTL_MS;
    this.maxTakeovers = options.maxTakeovers ?? DEFAULT_MAX_TAKEOVERS;
  }

  begin(request) {
    const {
      key,
      clientId,
      bodyDigest,
      partition,
      ownerId,
      now,
      leaseTtlMs = this.defaultLeaseTtlMs,
      ttlMs = this.defaultTtlMs,
    } = request;
    this.#assertRequest({ key, clientId, bodyDigest, partition, ownerId, now });

    let record = this.records.get(key);
    if (record !== undefined && this.#isCollectable(record, now)) {
      this.records.delete(key);
      record = undefined;
    }

    if (record === undefined) {
      const fresh = this.#newRecord({
        key,
        clientId,
        bodyDigest,
        partition,
        ownerId,
        now,
        leaseTtlMs,
        ttlMs,
      });
      this.records.set(key, fresh);
      return {
        status: Decision.EXECUTE,
        key,
        holder: ownerId,
        epoch: fresh.lease.epoch,
        leaseExpiresAt: fresh.lease.expiresAt,
        takeover: false,
        reusableResult: null,
        conflict: null,
      };
    }

    if (record.partition !== partition && leaseActive(record, now)) {
      return this.#conflict(record, {
        type: 'partition_mismatch',
        expectedPartition: record.partition,
        receivedPartition: partition,
      });
    }
    if (record.partition !== partition) {
      record.partition = partition;
      record.migrations += 1;
    }

    if (record.bodyDigest !== bodyDigest) {
      return this.#conflict(record, {
        type: 'digest_mismatch',
        expectedDigest: record.bodyDigest,
        receivedDigest: bodyDigest,
      });
    }

    if (record.state === State.COMPLETED) {
      return {
        status: Decision.REPLAY,
        key,
        holder: holderOf(record),
        epoch: record.lease === null ? null : record.lease.epoch,
        reusableResult: {
          serialized: record.result.serialized,
          resultDigest: record.result.resultDigest,
          completedAt: record.result.completedAt,
          completedBy: record.result.completedBy,
        },
        conflict: null,
      };
    }

    if (leaseActive(record, now)) {
      if (record.state === State.FAILED) {
        return this.#reject(record, {
          type: 'execution_failed',
          failure: record.failure,
        });
      }
      return {
        status: Decision.WAIT,
        key,
        holder: holderOf(record),
        epoch: record.lease.epoch,
        retryAfterMs: record.lease.expiresAt - now,
        reusableResult: null,
        conflict: null,
      };
    }

    if (record.takeovers >= this.maxTakeovers) {
      return this.#reject(record, {
        type: 'lease_flapping',
        takeovers: record.takeovers,
        maxTakeovers: this.maxTakeovers,
      });
    }

    record.takeovers += 1;
    record.lease = {
      owner: ownerId,
      epoch: record.lease === null ? 1 : record.lease.epoch + 1,
      expiresAt: now + leaseTtlMs,
    };
    record.state = State.EXECUTING;
    record.failure = null;
    record.clientId = clientId;
    return {
      status: Decision.EXECUTE,
      key,
      holder: ownerId,
      epoch: record.lease.epoch,
      leaseExpiresAt: record.lease.expiresAt,
      takeover: true,
      reusableResult: null,
      conflict: null,
    };
  }

  complete(request) {
    const { key, ownerId, epoch, result, now, ttlMs = this.defaultTtlMs } =
      request;
    const record = this.records.get(key);
    if (record === undefined) {
      return { status: CompleteStatus.UNKNOWN_KEY, key, holder: null };
    }
    const holder = holderOf(record);
    if (
      record.state !== State.EXECUTING ||
      record.lease === null ||
      record.lease.owner !== ownerId ||
      record.lease.epoch !== epoch
    ) {
      return {
        status: CompleteStatus.STALE,
        key,
        holder,
        epoch: record.lease === null ? null : record.lease.epoch,
      };
    }
    if (record.lease.expiresAt <= now) {
      return {
        status: CompleteStatus.LEASE_EXPIRED,
        key,
        holder,
        epoch: record.lease.epoch,
      };
    }

    let serialized;
    try {
      serialized = JSON.stringify(result);
      if (serialized === undefined) {
        throw new TypeError('result is not JSON-serializable');
      }
    } catch (error) {
      record.state = State.FAILED;
      record.failure = {
        type: 'result_serialization',
        message: error.message,
        failedAt: now,
        failedBy: ownerId,
      };
      return {
        status: CompleteStatus.SERIALIZATION_FAILED,
        key,
        holder,
        epoch: record.lease.epoch,
        failure: record.failure,
      };
    }

    record.state = State.COMPLETED;
    record.result = {
      serialized,
      resultDigest: digestResult(serialized),
      completedAt: now,
      completedBy: ownerId,
    };
    record.expiresAt = now + ttlMs;
    return {
      status: CompleteStatus.COMMITTED,
      key,
      holder,
      epoch: record.lease.epoch,
      resultDigest: record.result.resultDigest,
    };
  }

  cleanup(now) {
    const removed = [];
    for (const [key, record] of this.records) {
      if (this.#isCollectable(record, now)) {
        this.records.delete(key);
        removed.push(key);
      }
    }
    return { removed, remaining: this.records.size };
  }

  inspect(key) {
    const record = this.records.get(key);
    if (record === undefined) {
      return null;
    }
    return {
      key: record.key,
      clientId: record.clientId,
      partition: record.partition,
      bodyDigest: record.bodyDigest,
      state: record.state,
      holder: holderOf(record),
      epoch: record.lease === null ? null : record.lease.epoch,
      leaseExpiresAt: record.lease === null ? null : record.lease.expiresAt,
      takeovers: record.takeovers,
      migrations: record.migrations,
      expiresAt: record.expiresAt,
      result: record.result,
      failure: record.failure,
    };
  }

  #isCollectable(record, now) {
    return record.expiresAt <= now && !leaseActive(record, now);
  }

  #newRecord({
    key,
    clientId,
    bodyDigest,
    partition,
    ownerId,
    now,
    leaseTtlMs,
    ttlMs,
  }) {
    return {
      key,
      clientId,
      bodyDigest,
      partition,
      state: State.EXECUTING,
      lease: { owner: ownerId, epoch: 1, expiresAt: now + leaseTtlMs },
      takeovers: 0,
      migrations: 0,
      createdAt: now,
      expiresAt: now + ttlMs,
      result: null,
      failure: null,
    };
  }

  #conflict(record, basis) {
    return {
      status: Decision.CONFLICT,
      key: record.key,
      holder: holderOf(record),
      epoch: record.lease === null ? null : record.lease.epoch,
      reusableResult: null,
      conflict: basis,
    };
  }

  #reject(record, basis) {
    return {
      status: Decision.REJECT,
      key: record.key,
      holder: holderOf(record),
      epoch: record.lease === null ? null : record.lease.epoch,
      reusableResult: null,
      conflict: basis,
    };
  }

  #assertRequest({ key, clientId, bodyDigest, partition, ownerId, now }) {
    for (const [name, value] of Object.entries({
      key,
      clientId,
      bodyDigest,
      partition,
      ownerId,
    })) {
      if (typeof value !== 'string' || value.length === 0) {
        throw new TypeError(`${name} must be a non-empty string`);
      }
    }
    if (!Number.isFinite(now)) {
      throw new TypeError('now must be a finite number (ms epoch)');
    }
  }
}
