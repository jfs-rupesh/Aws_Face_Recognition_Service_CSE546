import time
import boto3
import os

aws_access_key_id =os.environ.get('key')
aws_secret_access_key = os.environ.get('key_acc')
region_name = 'us-east-1'
endpoint_url='https://sqs.us-east-1.amazonaws.com'

req_que_url = os.environ.get('req')
resp_que_url = os.environ.get('res')



ec2_app = boto3.Session(aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)

sqs = boto3.client('sqs', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, endpoint_url=endpoint_url, region_name=region_name)
# ec2 = boto3.client('ec2', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)
# ec2x = boto3.resource('ec2', aws_access_key_id= aws_access_key_id, aws_secret_access_key=aws_secret_access_key, region_name=region_name)

ec2x = ec2_app.resource('ec2', 'us-east-1')
ec2 = ec2_app.client('ec2', 'us-east-1')


running_instances = []
stopped_instances = []
starting_instances = []
stopping_instances = []
inside=False


def get_total_mssg():
    response = sqs.get_queue_attributes(
            QueueUrl=req_que_url,
            AttributeNames=[
                'ApproximateNumberOfMessages',
                'ApproximateNumberOfMessagesNotVisible'
            ]
        )

    num_visible_messages = int(response['Attributes']['ApproximateNumberOfMessages'])
    num_invisible_messages = int(response['Attributes']['ApproximateNumberOfMessagesNotVisible'])
    num_requests = num_visible_messages + num_invisible_messages

    return num_requests

def get_active_app_ins():
    cnt=0
    running_instances.clear()
    instances = ec2x.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['running']}])
    for instance in instances:
        if instance.id != 'i-03ef5c2b0f74d1c24' :
            running_instances.append(instance.id)
            cnt += 1
    #print("Running instances: ", cnt)
    return cnt

def get_stopped_ins():
    cnt=0
    stopped_instances.clear()
    instances = ec2x.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['stopped']}])
    for instance in instances:
        cnt += 1
        stopped_instances.append(instance.id)
    #print("Stopped instances: ",cnt)
    return cnt

def get_starting_ins():
    cnt=0
    starting_instances.clear()
    instances = ec2x.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['pending']}])
    for instance in instances:
        cnt += 1
        starting_instances.append(instance.id)
    #print("Pending : ",cnt)
    return cnt

def get_stopping_ins():
    cnt=0
    stopping_instances.clear()
    instances = ec2x.instances.filter(
        Filters=[{'Name': 'instance-state-name', 'Values': ['Stopping']}])
    for instance in instances:
        cnt += 1
        stopping_instances.append(instance.id)
    #print("Stopping: ",cnt)
    return cnt




def scale_up():
    total_run_ins = get_active_app_ins() + get_starting_ins()
    totalStoppedInstance = get_stopped_ins()
    get_stopping_ins()
    total_msg = get_total_mssg()

    # print("Start Logic")
    # print('total_msg: ', total_msg)
    # print('total: app_ins: ', total_run_ins)
    # print('total: stopped: ', totalStoppedInstance)


    if totalStoppedInstance==0 and get_stopping_ins() > 0:
        return

    if total_msg and total_msg > total_run_ins:
        t = 20 - (total_run_ins)
        st = total_msg - total_run_ins
        min_ins = min(t, st)
        min_ins = min(min_ins, totalStoppedInstance)
        # print('minimum instances to start: ', min_ins)

        if min_ins > 0 and  totalStoppedInstance >0:
            count = 0
            instanceID = []
            #print(len(stopped_instances))
            while count < min_ins:
                instanceID.append(stopped_instances.pop())
                count += 1
            if len(instanceID)>0:
                ec2.start_instances(InstanceIds = instanceID)



def clearRespQueue():
    response = sqs.purge_queue(
        QueueUrl=resp_que_url
    )
    # print("Response Queue Cleared")





def scale_down():
    get_starting_ins()
    get_stopping_ins()
    get_stopped_ins()
    msgs_in_req_que = get_total_mssg()
    app_ins = get_active_app_ins()
    diff = msgs_in_req_que - (app_ins)

    #print("Stop Logic")
    #print('total_msg: ', msgs_in_req_que)
    #print('total app_ins: ', app_ins)
    #print('diff: ', diff)
    #print(len(running_instances))

    count = 0
    if diff < 0:
        while count < abs(diff) and len(running_instances):
            inst_id = running_instances.pop()
            ec2.stop_instances(InstanceIds=[inst_id])
            count += 1

def initialize() :
    global  inside
    scale_up()
    time.sleep(5)
    if get_active_app_ins()-get_total_mssg() > 0:
        time.sleep(20)
        if get_active_app_ins() - get_total_mssg() > 0:
            scale_down()
            inside=True;


    if(inside and get_total_mssg()==0 and get_active_app_ins()==0):
        #print("inside finally")
        inside=False
        clearRespQueue()


while True :
    initialize()