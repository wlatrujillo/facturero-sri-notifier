import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as eventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as sns from 'aws-cdk-lib/aws-sns';
import * as snsSubscriptions from 'aws-cdk-lib/aws-sns-subscriptions';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as dynamodb from 'aws-cdk-lib/aws-dynamodb';
import * as path from 'path';

export class FactureroSriNotifierStack extends cdk.Stack {



  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // Create SNS topic for email notifications
    const emailTopic = new sns.Topic(this, 'FactureroSriNotifierTopic', {
      topicName: 'facturero-sri-notifier-topic',
      displayName: 'Facturero SRI Email Notifications'
    });

    const sendEmailFunction = new lambdaNodejs.NodejsFunction(this, 'SendEmailFunction', {
      entry: path.join(__dirname, '../lambda/send-email.ts'),
      handler: 'handler',
      runtime: cdk.aws_lambda.Runtime.NODEJS_24_X,
      timeout: cdk.Duration.seconds(30),
      environment: {
        SENDER_EMAIL: 'wladimir.trujillo.ec@gmail.com',
        TEST_BUCKET: 'prd-facturero-sri-vouchers-test',
        PRODUCTION_BUCKET: 'prd-facturero-sri-vouchers'
      }
    });

    // Subscribe the Lambda function to the SNS topic
    emailTopic.addSubscription(new snsSubscriptions.LambdaSubscription(sendEmailFunction));

    sendEmailFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendEmail', 'ses:SendRawEmail'],
      resources: ['*']
    }));

    const testBucket = s3.Bucket.fromBucketName(this, 'SriVouchersTestBucket', 'prd-facturero-sri-vouchers-test');
    const productionBucket = s3.Bucket.fromBucketName(this, 'SriVouchersProductionBucket', 'prd-facturero-sri-vouchers');

    testBucket.grantRead(sendEmailFunction);
    productionBucket.grantRead(sendEmailFunction);

    // Reference existing DynamoDB tables
    const latestStreamArnVouchersTable: string = 'arn:aws:dynamodb:us-east-1:030608081964:table/prd-facturero-sri-vouchers/stream/2026-02-17T17:56:56.997';
    const latestStreamVouchersTestTable: string = 'arn:aws:dynamodb:us-east-1:030608081964:table/prd-facturero-sri-vouchers-test/stream/2026-02-17T17:56:57.118';
    const vouchersTable = dynamodb.Table.fromTableAttributes(
      this,
      'VouchersTable',
      {
        tableName: 'prd-facturero-sri-vouchers',
        tableStreamArn: latestStreamArnVouchersTable
      }
    );

    const vouchersTestTable = dynamodb.Table.fromTableAttributes(
      this,
      'VouchersTestTable',
      {
        tableName: 'prd-facturero-sri-vouchers-test',
        tableStreamArn: latestStreamVouchersTestTable
      }
    );

    // Create Lambda function to process DynamoDB streams
    const streamProcessorFunction = new lambdaNodejs.NodejsFunction(this, 'StreamProcessorFunction', {
      entry: path.join(__dirname, '../lambda/stream-processor.ts'),
      handler: 'handler',
      runtime: cdk.aws_lambda.Runtime.NODEJS_24_X,
      timeout: cdk.Duration.seconds(60),
      environment: {
        TOPIC_ARN: emailTopic.topicArn
      }
    });

    // Add DynamoDB Stream event sources for both tables
    streamProcessorFunction.addEventSource(
      new eventSources.DynamoEventSource(vouchersTable, {
        startingPosition: cdk.aws_lambda.StartingPosition.LATEST,
        batchSize: 10,
        retryAttempts: 3
      })
    );

    streamProcessorFunction.addEventSource(
      new eventSources.DynamoEventSource(vouchersTestTable, {
        startingPosition: cdk.aws_lambda.StartingPosition.LATEST,
        batchSize: 10,
        retryAttempts: 3
      })
    );

    // Grant permissions to publish to SNS topic
    emailTopic.grantPublish(streamProcessorFunction);

    // Grant read permissions on DynamoDB streams
    vouchersTable.grantStreamRead(streamProcessorFunction);
    vouchersTestTable.grantStreamRead(streamProcessorFunction);
  }
}
