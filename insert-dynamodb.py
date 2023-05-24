
import boto3
import pandas as pd
import time
import json

a_data = pd.read_csv('D:\dev\Sunday-App\dynamodb-with-python-boto3-main\Results.csv')
a_key = ""
a_S_key = ""
region=""


AdvgCountries_json = json.loads(
pd.read_csv('D:\dev\Sunday-App\dynamodb-with-python-boto3-main\Results.csv').to_json(orient='records')
)
print(len(AdvgCountries_json))

lst_Dics = [{'item': AdvgCountries_json, 'table':'prod_Like'}]
def is_not_negative(record):
    if(record['customerId']):
        return True
    return False
my_filter = filter(is_not_negative, AdvgCountries_json)
result = list(my_filter)
 
print(len(result))
#Connect to DynamoDb Function
dynamodb = boto3.resource('dynamodb',
 aws_access_key_id="",
  aws_secret_access_key="",
   region_name="")
def insertDynamoItem (item_lst):
    dynamoTable = dynamodb.Table('prod_Like')
    for idx,record in enumerate(item_lst): 
        try:
            if(record['customerId'] and idx > 746):
                dynamoTable.put_item(Item=record)
            #else:
                #dynamoTable.put_item(Item=record)
            if(idx % 1000 == 0):
                print("Sleeping for 3 seconds.")
                time.sleep(3)
            if(idx % 100 == 0):
                print(idx)
        except Exception as e:
            print("An exception occurred => ")
            print(idx)
            print(record)
            print(e.message)
            print("---------------------------")
    print('Success')




insertDynamoItem(AdvgCountries_json)
