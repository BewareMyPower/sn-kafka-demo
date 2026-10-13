import argparse
import time

from confluent_kafka import Consumer, KafkaException


def caught_up(consumer, last_offsets, start_at_end, timeout=3.0):
    parts = consumer.assignment()
    if not parts:
        return False
    for tp in parts:
        try:
            _, hwm = consumer.get_watermark_offsets(tp, timeout=timeout, cached=False)
        except KafkaException:
            return False
        last = last_offsets.get(tp.partition)
        if last is not None:
            if last + 1 < hwm:
                return False
            continue
        # this partition delivered nothing yet: fall back to the committed offset
        try:
            committed = consumer.committed([tp], timeout=timeout)[0].offset
        except KafkaException:
            return False
        if committed >= hwm:
            continue
        if committed < 0 and start_at_end:
            continue
        return False
    return True


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Consume messages from Kafka until caught up')
    parser.add_argument('api_key_file', help='path to the file containing the API key/JWT')
    parser.add_argument('--topic', default='my-topic', help='topic to consume from (default: my-topic)')
    parser.add_argument('--group', default='sn-kafka-demo', help='consumer group id (default: sn-kafka-demo)')
    parser.add_argument('--earliest', action='store_true',
                        help="start from earliest when there is no committed offset (default: latest)")
    parser.add_argument('--report-every', type=int, default=1000,
                        help='print stats every N messages (default: 1000)')
    parser.add_argument('--idle-timeout', type=float, default=5.0,
                        help='consider caught up after this many seconds without new messages (default: 5)')
    parser.add_argument('--max-idle', type=float, default=60.0,
                        help='force stop after this many seconds without new messages (default: 60)')
    parser.add_argument('--debug', nargs='?', const='all', default=None, metavar='CONTEXTS',
                        help="enable librdkafka debug logging on stderr, e.g. 'broker,topic,msg' or 'all' (default with no value: all)")
    args = parser.parse_args()

    if args.report_every <= 0:
        parser.error('--report-every must be > 0')
    if args.idle_timeout <= 0:
        parser.error('--idle-timeout must be > 0')
    if args.max_idle < args.idle_timeout:
        parser.error('--max-idle must be >= --idle-timeout')

    bootstrap_servers = "c-j8bnvby-kc-lvjrj.serverless.aws-euc1-prod-snci-pool-slug.streamnative.aws.snio.cloud:9093"
    with open(args.api_key_file) as f:
        token = f.read().strip()
    service_account = "xyz1@o-wn080.auth.streamnative.cloud"

    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': args.group,
        'auto.offset.reset': 'earliest' if args.earliest else 'latest',
        'sasl.mechanism': 'PLAIN',
        'security.protocol': 'SASL_SSL',
        'sasl.username': service_account,
        'sasl.password': token,
    }
    if args.debug:
        conf['debug'] = args.debug

    consumer = Consumer(conf)
    consumer.subscribe([args.topic])

    start_at_end = not args.earliest
    count = 0
    total_size = 0
    min_offset = None
    max_offset = None
    last_offsets = {}
    start = time.monotonic()
    last_msg_time = start
    had_assignment = False

    try:
        while True:
            msg = consumer.poll(timeout=1.0)
            now = time.monotonic()

            if msg is None:
                assigned = bool(consumer.assignment())
                if assigned and not had_assignment:
                    had_assignment = True
                    last_msg_time = now
                    continue
                idle = now - last_msg_time
                if idle >= args.max_idle:
                    reason = 'no assignment' if not assigned else 'not caught up'
                    print(f'warning: {reason} after {idle:.0f}s, stopping')
                    break
                if idle >= args.idle_timeout and caught_up(consumer, last_offsets, start_at_end):
                    break
                continue

            if msg.error():
                print(f'Consumer error: {msg.error()}')
                continue

            last_msg_time = now
            count += 1
            total_size += len(msg.value() or b'')
            offset = msg.offset()
            min_offset = offset if min_offset is None else min(min_offset, offset)
            max_offset = offset if max_offset is None else max(max_offset, offset)
            last_offsets[msg.partition()] = offset

            if count % args.report_every == 0:
                elapsed = now - start
                print(f'count={count} size={total_size}B elapsed={elapsed:.2f}s '
                      f'offsets={min_offset}..{max_offset}')
    finally:
        consumer.close()

    elapsed = time.monotonic() - start
    offsets = 'n/a' if min_offset is None else f'{min_offset}..{max_offset}'
    print(f'consumed={count} size={total_size}B elapsed={elapsed:.2f}s offsets={offsets}')
