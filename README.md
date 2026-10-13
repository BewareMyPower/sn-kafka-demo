# sn-kafka-demo

使用 [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) 向 StreamNative Cloud Kafka 集群创建 topic、发送与消费消息的示例。

## 环境要求

- Python 3.9+
- [uv](https://docs.astral.sh/uv/)（用于管理依赖和虚拟环境）

## 安装环境

```bash
uv sync
```

该命令会创建 `.venv` 并安装 `confluent-kafka` 等依赖（见 `pyproject.toml`）。

## 配置

- SASL 凭据从文件读取：把 API key/JWT 保存到文本文件（单行即可），运行时通过命令行参数传入文件路径。
- topic 名称（三个脚本均有）、分区数与副本数（`create_topic.py`）、消费组与起始位点（`consumer.py`）均通过命令行参数配置。
- bootstrap servers、service account 直接写在脚本中，可按需修改。

## 运行

创建 topic：

```bash
uv run create_topic.py api-key-1.txt --topic my-topic --partitions 3
```

发送消息（按固定速率发送，`--rate` 每秒条数、`--size` 消息字节数、`--duration` 运行秒数；到时后停止发送并等待全部投递完成）：

```bash
uv run producer.py api-key-1.txt --topic my-topic --rate 100 --size 1024 --duration 30
```

开启 librdkafka debug 日志（输出到 stderr）：

```bash
uv run producer.py api-key-1.txt --debug            # 全部类别
uv run producer.py api-key-1.txt --debug broker,topic  # 指定类别
```

producer 显式开启幂等：`enable.idempotence=true`（librdkafka 默认为 false，开启后会自动调整 `acks=all`、`max.in.flight.requests.per.connection=5`、`retries=INT32_MAX`）。

运行结束输出统计：

```
sent=3000 delivered=3000 failed=0 size=1024B elapsed=30.05s
```

若凭据未授权对应 topic，则输出 `Failed to deliver message: Broker: Topic authorization failed`。

消费消息（`--topic` topic、`--group` 消费组、`--earliest` 无提交位点时从头开始消费（默认 latest）、`--report-every` 每 N 条打印一次统计、`--idle-timeout` 无新消息多少秒后校验各分区 high watermark 判定追上最新、`--max-idle` 兜底强制停止秒数）：

```bash
uv run consumer.py api-key-1.txt --topic my-topic --group my-group --earliest --report-every 1000
```

周期性统计（累计条数、累计 size、至今耗时、offset 范围）：

```
count=1000 size=1024000B elapsed=3.21s offsets=0..999
```

追上最新后停止，输出总耗时、总 offset 范围与总 size：

```
consumed=1234 size=1263616B elapsed=10.50s offsets=0..1233
```
