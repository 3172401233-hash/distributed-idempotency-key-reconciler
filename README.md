# distributed-idempotency-key-reconciler
协调跨分区请求幂等键、结果复用和冲突恢复

## 范围说明

本仓库只实现**离线状态协调器**：一个纯内存、纯函数的决策模块，输入请求键、
客户端、请求体摘要、处理租约、执行状态、结果摘要、TTL 和分区，输出每个请求
的决策（开始执行 / 复用结果 / 等待 / 拒绝冲突 / 清理过期）。它**不实现任何
网络服务**——没有 HTTP/RPC 服务端、没有存储引擎、没有跨进程复制。生产使用
时，由调用方把决策结果映射到自己的 API 层和持久化层。

目标运行时：Node.js 24.16.0（`package.json` 的 `engines` 为 `>=24.16.0`）。

## 使用

```js
import { IdempotencyCoordinator } from './src/coordinator.js';
import { digestBody } from './src/digest.js';

const coordinator = new IdempotencyCoordinator({
  leaseTtlMs: 30_000,   // 处理租约时长
  ttlMs: 86_400_000,    // 记录 TTL
  maxTakeovers: 3,      // 租约反复过期上限
});

const decision = coordinator.begin({
  key: 'order-123',
  clientId: 'client-a',
  bodyDigest: digestBody({ sku: 'book', qty: 1 }),
  partition: 'p0',
  ownerId: 'worker-1',
  now: Date.now(),
});
// decision.status: 'execute' | 'wait' | 'replay' | 'conflict' | 'reject'

if (decision.status === 'execute') {
  const result = await doWork();
  coordinator.complete({
    key: 'order-123',
    ownerId: 'worker-1',
    epoch: decision.epoch,
    result,
    now: Date.now(),
  });
}
```

## 决策语义

- **execute**：无记录或租约已过期，授予新租约（`epoch` 递增，`takeover` 标记是否为接管）。
- **wait**：相同键 + 相同摘要且租约活跃，返回当前持有者与 `retryAfterMs`。
- **replay**：相同键 + 相同摘要且已完成，返回可复用结果（序列化文本、结果摘要、完成者）。
- **conflict**：相同键但请求体摘要不同（`digest_mismatch`），或租约活跃时跨分区
  到达（`partition_mismatch`）；冲突依据在 `conflict` 字段。
- **reject**：结果序列化失败后相同摘要重试（`execution_failed`），或租约反复
  过期超过 `maxTakeovers`（`lease_flapping`）。

`complete` 使用 `(ownerId, epoch)` 围栏：旧实例晚到的提交返回 `stale`，不会
覆盖新持有者的结果；租约过期后的提交返回 `lease_expired`；结果无法 JSON
序列化时记录被标记为失败（`serialization_failed`）。

`cleanup(now)` 只移除 TTL 过期且租约也不活跃的记录，与并发 `begin` 竞争安全
（`begin` 内部对过期记录做同样的惰性回收）。

跨分区迁移：租约活跃时拒绝；租约过期后允许迁移，`partition` 更新且
`migrations` 计数递增。

## 测试

```sh
node --test
```

覆盖：并发重试、不同请求体冲突、租约接管、旧结果晚到、序列化失败、租约反复
过期、跨分区迁移、清理竞争。
