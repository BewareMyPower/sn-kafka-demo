import argparse

from confluent_kafka import Producer

def on_delivery(err, msg):
    if err is not None:
        print(f'Failed to deliver message: {err.str()}')
    else:
        print(f'Produced to: {msg.topic()} [{msg.partition()}] @ {msg.offset()}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Produce a message to Kafka')
    parser.add_argument('api_key_file', help='path to the file containing the API key/JWT')
    parser.add_argument('--topic', default='my-topic', help='topic to produce to (default: my-topic)')
    parser.add_argument('--debug', nargs='?', const='all', default=None, metavar='CONTEXTS',
                        help="enable librdkafka debug logging on stderr, e.g. 'broker,topic,msg' or 'all' (default with no value: all)")
    args = parser.parse_args()

    bootstrap_servers = "c-j8bnvby-kc-lvjrj.serverless.aws-euc1-prod-snci-pool-slug.streamnative.aws.snio.cloud:9093"
    topic = args.topic
    with open(args.api_key_file) as f:
        token = f.read().strip()
    service_account = "xyz1@o-wn080.auth.streamnative.cloud"

    conf = {
        'bootstrap.servers': bootstrap_servers,
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': service_account,
        'sasl.password': token,
    }
    if args.debug:
        conf['debug'] = args.debug

    producer = Producer(conf)


    producer.produce(topic, value='hello python', callback=on_delivery)
    producer.poll()
