import argparse
import sys

from confluent_kafka.admin import AdminClient, NewTopic


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Create a Kafka topic')
    parser.add_argument('api_key_file', help='path to the file containing the API key/JWT')
    parser.add_argument('--topic', required=True, help='name of the topic to create')
    parser.add_argument('--partitions', type=int, default=3, help='number of partitions (default: 3)')
    parser.add_argument('--replication-factor', type=int, default=3, help='replication factor (default: 3)')
    args = parser.parse_args()

    bootstrap_servers = "c-j8bnvby-kc-lvjrj.serverless.aws-euc1-prod-snci-pool-slug.streamnative.aws.snio.cloud:9093"
    with open(args.api_key_file) as f:
        token = f.read().strip()
    service_account = "xyz1@o-wn080.auth.streamnative.cloud"

    admin = AdminClient({
        'bootstrap.servers': bootstrap_servers,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': service_account,
        'sasl.password': token,
    })

    new_topic = NewTopic(args.topic, num_partitions=args.partitions, replication_factor=args.replication_factor)
    futures = admin.create_topics([new_topic])

    for name, future in futures.items():
        try:
            future.result()
            print(f'Created topic: {name} (partitions={args.partitions}, replication_factor={args.replication_factor})')
        except Exception as e:
            print(f'Failed to create topic {name}: {e}')
            sys.exit(1)
