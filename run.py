import boto3
import os
import pandas

global ec2_client
ec2_client = boto3.client("ec2", region_name="us-east-1")
global s3_client
s3_client = boto3.client('s3', region_name="us-east-1")

def create_key_pair():
  try:
    key_pair = ec2_client.create_key_pair(KeyName="laba")
    private_key = key_pair["KeyMaterial"]

    with os.fdopen(os.open("laba.pem", os.O_WRONLY | os.O_CREAT, 0o400), "w+") as handle:
      handle.write(private_key)
  except Exception as e:
    print(f"Error: {e}")
#create_key_pair()


def security_group():
  response = ec2_client.describe_vpcs()
  vpc_id = response.get('Vpcs', [{}])[0].get('VpcId', '')
  try:
    response = ec2_client.create_security_group(GroupName='lab4',
                                                Description='DESCRIPTION',
                                                VpcId=vpc_id)
    security_group_id = response['GroupId']
    print('Security Group Created %s in vpc %s.' % (security_group_id, vpc_id))

    data = ec2_client.authorize_security_group_ingress(
      GroupId=security_group_id,
      IpPermissions=[
        {'IpProtocol': 'tcp',
         'FromPort': 22,
         'ToPort': 22,
         'IpRanges': [{'CidrIp': '0.0.0.0/0'}]}
      ])
    print('Ingress Successfully Set %s' % data)
  except Exception as e:
    print(f"Error: {e}")


#security_group()


def create_instance():
  try:
    instances = ec2_client.run_instances(
      ImageId="ami-0715c1897453cabd1",
      MinCount=1,
      MaxCount=1,
      InstanceType="t3.micro",
      KeyName="laba",
      SecurityGroupIds=[
        'sg-05549a5a2f85f1b7f'
      ]
    )
    print(instances["Instances"][0]["InstanceId"])
  except Exception as e:
    print(f"Error: {e}")

#create_instance()

def get_public_ip(instance_id):
  try:
    reservations = ec2_client.describe_instances(InstanceIds=[instance_id]).\
      get("Reservations")
    for reservation in reservations:
      for instance in reservation['Instances']:
        print(instance.get("PublicIpAddress"))
  except Exception as e:
    print(f"Error: {e}")


#get_public_ip('i-06d7159c22e335075')


def get_running_instances():
  tmp= {}
  try:
    reservations = ec2_client.describe_instances(Filters=[
      {
        "Name": "instance-state-name",
        "Values": ["running"],
      }
    ]).get("Reservations")
    for reservation in reservations:
      for instance in reservation["Instances"]:
        instance_id = instance["InstanceId"]
        instance_type = instance["InstanceType"]
        public_ip = instance["PublicIpAddress"]
        private_ip = instance["PrivateIpAddress"]
        tmp[instance_id]=[public_ip]
        print(f"Running:{instance_id}, {instance_type}, {public_ip}, {private_ip}")
  except Exception as e:
    print(f"Error: {e}")
  return tmp
      
#get_running_instances()

def ssh(instance_id):
  tmp = get_running_instances()
  if instance_id in tmp:
    ip = tmp[instance_id][0]
    print(f"Your ssh command: ssh -i laba.pem ec2-user@{ip}")
  else:
    print("You cannot access to this instance.It is stopped or non-existent.")

ssh('i-06d7159c22e335075')

def stop_instance(instance_id):
  try:
    response = ec2_client.stop_instances(InstanceIds=[instance_id])
    print(response)
  except Exception as e:
    print(f"Error {e}")

#stop_instance('i-0ffd17ef2c0a58fdc')

def terminate_instance(instance_id):
  try:
    response = ec2_client.terminate_instances(InstanceIds=[instance_id])
    print(response)
  except Exception as e:
    print(f"Error: {e}")

#terminate_instance('i-0ffd17ef2c0a58fdc')

def get_instance_info(instance_id):
  try:
    response = ec2_client.describe_instance_status(InstanceIds=[instance_id])
    print(f"Instance {instance_id} info:")
    print(response)
  except Exception as e:
    print(f"Error: {e}")

#get_instance_info('i-0ffd17ef2c0a58fdc')
def bucket_list():
  tmp =[]
  try:
    response = s3_client.list_buckets()
    #print('Existing buckets:')
    for bucket in response['Buckets']:
      tmp.append(bucket["Name"])
      #print(f'- {bucket["Name"]}')
  except Exception as e:
    print(f"Error: {e}")
  return tmp

def bucket_element_exists(bucket_name, s3_obj_name):
  try:
    s3_client.get_object(Bucket=bucket_name, Key=s3_obj_name)
  except:
    return False
  return True

def bucket_exists(bucket_name):
  tmp = bucket_list()
  if bucket_name not in tmp:
    return False
  return True

#bucket_list()
def create_bucket(bucket_name):
  tmp = bucket_list()
  if bucket_name in tmp:
    print("Error, such backet already exists")
    return
  try:
    response = s3_client.create_bucket(Bucket=bucket_name)
    print(response)
  except Exception as e:
    print(f"Error: {e}")

#create_bucket("laba4")

def upload(file_name, bucket_name, s3_obj_name):
  try:
    if not bucket_exists(bucket_name):
      print(F"Error. No such bucket {bucket_name}")
      return
    if bucket_element_exists(bucket_name, file_name):
      print(F"Error.File already exists {file_name}")
      return
    else:
      with open(file_name, "rb") as f:
        s3_client.upload_fileobj(f, bucket_name, file_name)
        print('File was uploaded successfully')
  except Exception as e:
    print(f"Error: {e}")

#upload('file1.txt', 'laba4', 'file1')


def read_data(bucket, file):
  try:
    obj = s3_client.get_object(
      Bucket=bucket,
      Key=file
    )
    data = pandas.read_csv(obj['Body'])
    print('Printing the data frame...')
    print(data.head(10))
  except Exception as e:
    print(f"Error: {e}")

#read_data('datalabtwo','data.csv' )

def destroy_bucket(bucket_name):
  try:
    response = s3_client.delete_bucket(Bucket=bucket_name)
    print(response)
  except Exception as e:
    print(f"Error: {e}")

#destroy_bucket('laba4')
