# AWS DynamoDBtoCSV (special fork with lambda invoke instead of write to csv)

This application will export the content of a DynamoDB table into AWS Lambda.

This software is governed by the Apache 2.0 license.

## Usage
```
npm install
node dynamoDBtoCSV.js --table inbox_conversations_CLONE -p quoka-live --region=eu-central-1 -v '{}' --lambdaARN=InboxTriggerSQS --size=1000 --lastKey=53fc5fde94e348ecba12129b3f6954be --batch=100
```
where:
- table - DynamoDB table name
- p - profile in AWS
- v - the expression for filtering on the primary key
- lambdaARN - name of Lambda to be invoked
- size - when to flush data before scanning next part from DynamoDB
- lastKey - last DynamoDB logged id, used to start from specific position, if missing, starting from begining
- batch - maximum size of batch with IDs to be sent to Lambda
