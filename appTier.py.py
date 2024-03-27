import base64
import boto3
import os
import time
import datetime
import io

aws_access_key_id = os.environ.get('key')
aws_secret_access_key = os.environ.get('key_acc')
region_name = 'us-east-1'
request_queue_url = os.environ.get('req')
response_queue_url = os.environ.get('res')
endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
sqs = boto3.client('sqs', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                   endpoint_url=endpoint_url, region_name=region_name)
s3_client = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                         region_name=region_name)
s3 = boto3.resource(
    service_name='s3',
    region_name=region_name,
    aws_access_key_id=aws_access_key_id,
    aws_secret_access_key=aws_secret_access_key
)


def receiveMessages():
    #print("in recieved messages")
    response = sqs.receive_message(
        QueueUrl=request_queue_url,
        AttributeNames=[
            'SentTimestamp'
        ],
        MaxNumberOfMessages=1,
        MessageAttributeNames=[
            'All'
        ],
        VisibilityTimeout=30,
    )

    if 'Messages' in response:
        #print("new messages found in request queus")
        return response['Messages']
    else:
        time.sleep(1)
        return receiveMessages()


def deleteMessage(receipt_handle):
    sqs.delete_message(
        QueueUrl=request_queue_url,
        ReceiptHandle=receipt_handle
    )


def decodeMessage(fName, msg):
    decodeit = open(fName, 'wb')
    decodeit.write(base64.b64decode((msg)))
    decodeit.close()


def sendMessageInResponseQueue(fName, msg):
    endpoint_url = 'https://sqs.us-east-1.amazonaws.com'
    sqs = boto3.client('sqs', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key,
                       endpoint_url=endpoint_url, region_name=region_name)
    resp = sqs.send_message(
        QueueUrl=response_queue_url,
        MessageBody=(
                fName + " " + msg
        )
    )



def upload_to_s3_input_bucket(s3, bucket_name, image_name, image_source):
    s3.Object(bucket_name, image_name).upload_file(Filename=image_source)


def upload_to_s3_output_bucket(s3, bucket_name, image_name, image_source):
    s3.Object(bucket_name, image_name).upload_file(Filename=image_source)


def initialize():
    message = receiveMessages()[0]
    #print(message)
    receipt_handle = message['ReceiptHandle']
    fileName, encodedMssg = message['Body'].split()
    #print(fileName)
    msg_value = bytes(encodedMssg, 'utf-8')
    with open("outputFile.bin", "wb") as file:
        file.write(msg_value)
    file = open('outputFile.bin', 'rb')
    byte = file.read()
    file.close()
    qp = base64.b64decode(byte)
    with open("Imagefile", "wb") as imageFile:
        imageFile.write(qp)

    #print("Filename:", fileName)
    out = os.popen("python3 face_recognition.py Imagefile")
    result = out.read().strip()
    #print("Result:", result)
    ClassificationResultFile = open("ClassificationResult", "w")
    ClassificationResultFile.write(result)
    ClassificationResultFile.close()

    upload_to_s3_input_bucket(s3,"in-bucket",fileName, "Imagefile")
    upload_to_s3_output_bucket(s3,"out-bucket",fileName, "ClassificationResult")

    sendMessageInResponseQueue(fileName, result)
    deleteMessage(receipt_handle)

while True:
    initialize()