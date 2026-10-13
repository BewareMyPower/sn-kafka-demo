# sn-kafka-demo

使用 [confluent-kafka](https://github.com/confluentinc/confluent-kafka-python) 向 StreamNative Cloud Kafka 集群创建 topic、发送消息的示例。

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
- topic 名称（两个脚本）、分区数与副本数（`create_topic.py`）均通过命令行参数配置。
- bootstrap servers、service account 直接写在脚本中，可按需修改。

## 运行

创建 topic：

```bash
uv run create_topic.py api-key-1.txt --topic my-topic --partitions 3
```

发送消息：

```bash
uv run producer.py api-key-1.txt --topic my-topic
```

开启 librdkafka debug 日志（输出到 stderr）：

```bash
uv run producer.py api-key-1.txt --debug            # 全部类别
uv run producer.py api-key-1.txt --debug broker,topic  # 指定类别
```

producer 默认开启幂等（`enable.idempotence`），无需额外配置。

发送成功时输出：

```
Produced to: my-topic [0] @ 0
```

若凭据未授权对应 topic，则输出 `Failed to deliver message: Broker: Topic authorization failed`。
