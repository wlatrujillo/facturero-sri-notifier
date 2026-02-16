import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as eventSources from 'aws-cdk-lib/aws-lambda-event-sources';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as lambdaNodejs from 'aws-cdk-lib/aws-lambda-nodejs';
import * as sqs from 'aws-cdk-lib/aws-sqs';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as path from 'path';

export class FactureroSriNotifierStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const queue = new sqs.Queue(this, 'FactureroSriNotifierQueue', {
      visibilityTimeout: cdk.Duration.seconds(300)
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

    sendEmailFunction.addEventSource(new eventSources.SqsEventSource(queue, {
      batchSize: 10
    }));

    queue.grantConsumeMessages(sendEmailFunction);

    sendEmailFunction.addToRolePolicy(new iam.PolicyStatement({
      actions: ['ses:SendEmail', 'ses:SendRawEmail'],
      resources: ['*']
    }));

    const testBucket = s3.Bucket.fromBucketName(this, 'SriVouchersTestBucket', 'prd-facturero-sri-vouchers-test');
    const productionBucket = s3.Bucket.fromBucketName(this, 'SriVouchersProductionBucket', 'prd-facturero-sri-vouchers');

    testBucket.grantRead(sendEmailFunction);
    productionBucket.grantRead(sendEmailFunction);
  }
}
