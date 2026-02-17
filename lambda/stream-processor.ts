import { DynamoDBStreamEvent, DynamoDBRecord } from 'aws-lambda';
import { SQSClient, SendMessageCommand } from '@aws-sdk/client-sqs';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { AttributeValue } from '@aws-sdk/client-dynamodb';

const sqsClient = new SQSClient({});
const QUEUE_URL = process.env.QUEUE_URL!;

export const handler = async (event: DynamoDBStreamEvent): Promise<void> => {
  console.log('Processing DynamoDB Stream event', JSON.stringify(event, null, 2));

  for (const record of event.Records) {
    try {
      await processRecord(record);
    } catch (error) {
      console.error('Error processing record:', error);
      throw error;
    }
  }
};

async function processRecord(record: DynamoDBRecord): Promise<void> {
   // Get table name from ARN
  const eventSourceARN = record.eventSourceARN;
  const tableName = eventSourceARN?.split(':table/')[1]?.split('/stream')[0];

  console.log('Event triggered from table:', tableName);
  console.log('Record event name:', record.eventName);
  // Only process MODIFY events
  if (record.eventName !== 'MODIFY') {
    console.log('Skipping non-MODIFY event:', record.eventName);
    return;
  }

  const oldImage = record.dynamodb?.OldImage;
  const newImage = record.dynamodb?.NewImage;

  if (!oldImage || !newImage) {
    console.log('Missing old or new image');
    return;
  }

  // Unmarshall the DynamoDB records
  const oldData = unmarshall(oldImage as any);
  const newData = unmarshall(newImage as any);

  const oldStatus = oldData.status;
  const newStatus = newData.status;

  console.log(`Status change: ${oldStatus} -> ${newStatus}`);

  // Check if status changed from RECEIVED or PROCESSING to AUTHORIZED
  if ((oldStatus === 'RECEIVED' || oldStatus === 'PROCESSING') && newStatus === 'AUTHORIZED') {
    console.log(`Status changed from ${oldStatus} to ${newStatus}, sending SQS message`);
    await sendSqsMessage(newData, tableName);
  }
}

async function sendSqsMessage(data: any, tableName: string = 'facturero-sri-vouchers-test' ): Promise<void> {
  const message = {
    eventType: 'STATUS_CHANGE',
    status: data.status,
    accessKey: data.accessKey,
    environment: tableName.includes('test') ? 'test' : 'production',
    timestamp: new Date().toISOString()
  };

  const command = new SendMessageCommand({
    QueueUrl: QUEUE_URL,
    MessageBody: JSON.stringify(message),
    MessageAttributes: {
      eventType: {
        DataType: 'String',
        StringValue: 'STATUS_CHANGE'
      },
      status: {
        DataType: 'String',
        StringValue: data.status
      }
    }
  });

  const result = await sqsClient.send(command);
  console.log('SQS message sent:', result.MessageId);
}
