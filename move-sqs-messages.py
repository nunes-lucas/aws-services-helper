import argparse
import boto3

sqs = boto3.resource('sqs')


def move_messages(src_queue, dst_queue):

    print('Moving messages from:  %s to: %s' % (src_queue, dst_queue))

    from_q = sqs.get_queue_by_name(QueueName=src_queue)
    to_q = sqs.get_queue_by_name(QueueName=dst_queue)

    while True:
        messages = from_q.receive_messages(MaxNumberOfMessages=10)

        for message in messages:
            print('Processing message %s' % message.message_id)
            send_result = to_q.send_message(MessageBody=message.body)

            if send_result['ResponseMetadata']['HTTPStatusCode'] == 200:
                delete_result = message.delete()
                if delete_result['ResponseMetadata']['HTTPStatusCode'] != 200:
                    print('Failed to delete message: %s cause: %s'
                          % (message.message_id, str(delete_result)))
            else:
                print('Failed to move message: %s cause: %s'
                      % (message.message_id, str(send_result)))

        if len(messages) <= 0:
            print('Finish moving')
            break


def parse_args():
    parser = argparse.ArgumentParser(description="Move messages between SQS queues.")
    parser.add_argument('-s', '--src', required=True, help='Name of the source queue.')
    parser.add_argument('-d', '--dst', required=True, help='Name of the destination queue.')
    return parser.parse_args()


if __name__ == '__main__':
    try:
        args = parse_args()
        move_messages(args.src, args.dst)
    except KeyboardInterrupt:
        print('Interrupted')
