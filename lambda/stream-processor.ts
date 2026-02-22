import { DynamoDBStreamEvent, DynamoDBRecord } from 'aws-lambda';
import { SNSClient, PublishCommand } from '@aws-sdk/client-sns';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import { AttributeValue } from '@aws-sdk/client-dynamodb';

const snsClient = new SNSClient({});
const TOPIC_ARN = process.env.TOPIC_ARN!;

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
    console.log(`Status changed from ${oldStatus} to ${newStatus}, publishing to SNS`);
    await publishToSns(newData, tableName);
  }
}

async function publishToSns(data: any, tableName: string = 'facturero-sri-vouchers-test' ): Promise<void> {
  const message = {
    eventType: 'STATUS_CHANGE',
    status: data.status,
    accessKey: data.accessKey,
    environment: tableName.includes('test') ? 'test' : 'production',
    timestamp: new Date().toISOString()
  };

  const command = new PublishCommand({
    TopicArn: TOPIC_ARN,
    Message: JSON.stringify(message),
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

  const result = await snsClient.send(command);
  console.log('SNS message published:', result.MessageId);
}
