# nats_rpc

`nats_rpc` 是一个独立的 Go 库，负责基于 NATS 实现：

- RPC request/reply
- notify publish/subscribe
- 基于 protobuf 的统一信封协议
- 可注册的消息号与 protobuf 类型映射
- 可选的 typed router

核心库不依赖 GoFrame，也不依赖当前仓库的私有 `pbmsg` 协议。

## 目录

- `proto/natsrpc/v1`: 公开 protobuf 协议
- `registry`: `msg_id <-> proto.Message` 注册中心
- 根包 `natsrpc`: client/server/router/subscription/options
- `adapters/goframe`: GoFrame 适配层

## 核心思路

业务协议仍然使用 protobuf。

NATS 上传输的统一信封也使用 protobuf：

- `msg_id`: 业务消息号
- `rpc_id`: 请求流水号
- `body`: protobuf 序列化后的业务消息体
- `code/message`: 远端错误信息
- `trace_id`: 链路追踪标识
- `headers`: 扩展头

## 快速示例

```go
reg := registry.New()
client := natsrpc.NewClient(nc, natsrpc.WithRegistry(reg))
server := natsrpc.NewServer(nc, natsrpc.WithRegistry(reg))
router := natsrpc.NewRouter(reg)
```

## 说明

当前版本先完成独立库抽取，主项目原有调用点尚未整体切换到这个新库。
