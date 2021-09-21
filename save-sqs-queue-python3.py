import argparse
import boto3
import json
import os

parser = argparse.ArgumentParser(description='Saves all messages from an AWS SQS queue into a folder.')

parser.add_argument(
    '-q', '--queue', dest='queue', type=str, required=True,
    help='The name of the AWS SQS queue to save.')

parser.add_argument(
    '-r', '--region', dest='aws_region', type=str, required=True,
    help='The AWS region where the queue is located.')

parser.add_argument(
    '-k', '--key', dest='aws_key', type=str, required=True,
    help='Your AWS account key.')

parser.add_argument(
    '-s', '--secret', dest='aws_secret', type=str, required=True,
    help='Your AWS account secret.')
    
parser.add_argument(
    '-o', '--output', dest='output', type=str, default='queue-messages',
    help='The output folder for saved messages.')

parser.add_argument(
    '-d', '--delete', dest='delete', default=False, action='store_true',
    help='Whether or not to delete saved messages from the queue.')

parser.add_argument(
    '-v', '--visibility', dest='visibility', type=int, default=10,
    help='The message visibility timeout for saved messages.')
    
args = parser.parse_args()

if not os.path.exists(args.output):
    os.makedirs(args.output)

   
sess = boto3.session.Session(aws_access_key_id=args.aws_key, aws_secret_access_key=args.aws_secret, region_name=args.aws_region)
sqs = sess.resource("sqs")

queue = sqs.Queue(args.queue)

# Process messages by printing out body
msglist = []
filename = os.path.join(args.output, "mensagens")
with open(filename, 'w') as f:

    while True:
        messages = queue.receive_messages(VisibilityTimeout=args.visibility)
        if len(messages) == 0: break
        print('Amount of existing Queue messages',len(messages))
        for msg in messages:
            msglist.append(json.loads(msg.body))
            #print(msg.body)
            if args.delete:
                message.delete()
             
    json.dump(msglist, f, indent=4)
    
    
