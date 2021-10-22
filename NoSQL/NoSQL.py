#!/usr/bin/env python
# coding: utf-8

# In[1]:


from json import load
import boto3
import csv


# In[2]:


s3_bucket_name = "datacont-snagare"
table_name = "DataTable"


# In[3]:


# Change config.json to update the credentials
with open("config.json") as config:
    aws_config = load(config)


# In[4]:


s3 = boto3.resource("s3", **aws_config)
try:
    s3.create_bucket(Bucket=s3_bucket_name, CreateBucketConfiguration={
    "LocationConstraint": aws_config["region_name"]})
except Exception as e:
    print (e)


# In[5]:


dyndb = boto3.resource("dynamodb", **aws_config)


# In[6]:


# The first time that we define a table
try:
    table = dyndb.create_table(
        TableName='DataTable',
        KeySchema=[
            {
                'AttributeName': 'Id',
                'KeyType': 'HASH'
            },
            {
                'AttributeName': 'Temp',
                'KeyType': 'RANGE'
            },
        ],
        AttributeDefinitions=[
            {
                'AttributeName': 'Id',
                'AttributeType': 'S'
            },
            {
                'AttributeName': 'Temp',
                'AttributeType': 'S'
            },
        ],
        ProvisionedThroughput={
            'ReadCapacityUnits': 5,
            'WriteCapacityUnits': 5
        }
    )
except Exception as e:
    print (e)
    #if there is an exception, the table may already exist. if so...
    table = dyndb.Table("DataTable")


# In[7]:


table.meta.client.get_waiter('table_exists').wait(TableName=table_name)


# In[8]:


with open("experiments.csv") as csvfile: 
    csvf = csv.reader(csvfile, delimiter=',', quotechar='|')
    next(csvf)
    for item in csvf:
        print(item)
        body = open(item[4], 'rb')
        s3.Object(s3_bucket_name, item[4]).put(Body=body)
        s3.Object(s3_bucket_name, item[4]).Acl().put(ACL='public-read')
        metadata_item = {
            'Id': item[0], 
            'Temp':item[1],
            'Conductivity': item[2],
            'Concentration':item[3],
            'URL':f"https://{s3_bucket_name}.s3.{aws_config['region_name']}.amazonaws.com/{item[4]}"
        }
        try:
            table.put_item(Item=metadata_item)
        except:
            print("Item already exists!")


# In[9]:


response = table.get_item(
    Key={
        'Id': '3',
        'Temp': '-2.93',
    }
)
item = response['Item']
print(item)


# In[10]:


print(response)

