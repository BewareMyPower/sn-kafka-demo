import argparse
import time

from confluent_kafka import Producer

stats = {'delivered': 0, 'failed': 0}


def on_delivery(err, msg):
    if err is not None:
        stats['failed'] += 1
        print(f'Failed to deliver message: {err.str()}')
    else:
        stats['delivered'] += 1


def make_payload(seq, size):
    header = f'{seq:08d},'.encode()
    return (header + b'x' * size)[:size]


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Produce messages to Kafka at a fixed rate')
    parser.add_argument('api_key_file', help='path to the file containing the API key/JWT')
    parser.add_argument('--topic', default='my-topic', help='topic to produce to (default: my-topic)')
    parser.add_argument('--rate', type=float, default=100.0, help='messages per second (default: 100)')
    parser.add_argument('--size', type=int, default=1024, help='message payload size in bytes (default: 1024)')
    parser.add_argument('--duration', type=float, default=10.0,
                        help='how long to keep sending, in seconds (default: 10)')
    parser.add_argument('--debug', nargs='?', const='all', default=None, metavar='CONTEXTS',
                        help="enable librdkafka debug logging on stderr, e.g. 'broker,topic,msg' or 'all' (default with no value: all)")
    args = parser.parse_args()

    if args.rate <= 0:
        parser.error('--rate must be > 0')
    if args.size < 0:
        parser.error('--size must be >= 0')
    if args.duration < 0:
        parser.error('--duration must be >= 0')

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
        'enable.idempotence': True,
    }
    if args.debug:
        conf['debug'] = args.debug

    producer = Producer(conf)

    interval = 1.0 / args.rate
    start = time.monotonic()
    deadline = start + args.duration
    next_send = start
    seq = 0
    sent = 0

    while True:
        now = time.monotonic()
        if now >= deadline:
            break
        if now < next_send:
            time.sleep(min(next_send, deadline) - now)
            continue

        payload = make_payload(seq, args.size)
        while True:
            try:
                producer.produce(topic, value=payload, callback=on_delivery)
                break
            except BufferError:
                producer.poll(0.1)
        producer.poll(0)

        seq += 1
        sent += 1
        next_send += interval

    producer.flush()

    elapsed = time.monotonic() - start
    print(f'sent={sent} delivered={stats["delivered"]} failed={stats["failed"]} '
          f'size={args.size}B elapsed={elapsed:.2f}s')
