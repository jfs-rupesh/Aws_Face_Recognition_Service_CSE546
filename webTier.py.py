import boto3
from flask import Flask, request
import base64
import os

app = Flask(__name__)
res = dict()
aws_access_key_id =os.environ.get('key')
aws_secret_access_key = os.environ.get('key_acc')
region_name = 'us-east-1'
endpoint_url='https://sqs.us-east-1.amazonaws.com'

request_queue_url = os.environ.get('req')

response_queue_url = os.environ.get('res')
endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
sqs = boto3.client('sqs', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                   endpoint_url=endpoint_url, region_name=region_name)
s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


@app.route('/', methods=["POST"])
async def uploadImage():
    if 'inputFile' not in request.files:
        return "No inputFile provided", 400

    input_file = request.files['inputFile']
    if input_file.filename == '':
        return "No selected file", 400

    filename, _ = input_file.filename.rsplit('.', 1)
    filename += ".jpg"
    try:
        byteform = base64.b64encode(input_file.read())
        value = str(byteform, 'utf-8')
        str_byte = filename + " " + value
        sqs.send_message(
            QueueUrl=request_queue_url,
            MessageBody=(
                str_byte
            )
        )

        output = await get_correct_response(filename)

        result_message = f"{filename}:{output}"
        return result_message

    except Exception as e:
        print(str(e))
        return "Something went wrong " + str(e)


def get_number_of_msgs_in_res_queue():
    response = sqs.get_queue_attributes(
        QueueUrl=response_queue_url,
        AttributeNames=[
            'ApproximateNumberOfMessages',
            'ApproximateNumberOfMessagesNotVisible'
        ]
    )

    return int(response['Attributes']['ApproximateNumberOfMessages'])


async def get_correct_response(image):

    while True:

        if image in res.keys():
            return res[image]

        response = sqs.receive_message(
            QueueUrl=response_queue_url,
            MaxNumberOfMessages=10,
            MessageAttributeNames=[
                'All'
            ],
        )

        if 'Messages' in response:
            msgs = response['Messages']
            for msg in msgs:
                msg_body = msg['Body']
                res_image = msg_body.split(" ")[0]

                res[res_image] = msg_body.split(" ")[1]
                receipt_handle = msg['ReceiptHandle']

                sqs.delete_message(
                    QueueUrl=response_queue_url,
                    ReceiptHandle=receipt_handle
                )

                if res_image == image:
                    return res[res_image]


if __name__ == "__main__":
    app.run()