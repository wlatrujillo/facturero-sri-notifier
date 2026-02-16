import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SESClient, SendRawEmailCommand } from '@aws-sdk/client-ses';
import type { SQSHandler } from 'aws-lambda';


type EmailPayload = {
    subject?: string;
    body?: string;
    accessKey: string;
    environment?: 'test' | 'production';
};

const s3 = new S3Client({});
const ses = new SESClient({});


const getPayload = (body: string): Required<EmailPayload> => {
    try {
        const parsed = JSON.parse(body) as EmailPayload;
        return {
            subject: parsed.subject?.trim() || 'Notification',
            body: parsed.body?.trim() || body,
            accessKey: parsed.accessKey,
            environment: parsed.environment || 'test'
        };
    } catch {
        return {
            subject: 'Notification',
            body,
            accessKey: '',
            environment: 'test'
        };
    }
};

const getCompanyIdFromAccessKey = (accessKey: string): string => {
    // accessKey format: ddmmyyyyvoucherTypeCompanyIdEnviromentTypeEstablishmentBranchSequenceNumber
    // Example: 010220260117190042180011001100000000001
    const companyId = accessKey.substring(10, 23);
    return companyId;
};

export const handler: SQSHandler = async (event) => {
    const sender = process.env.SENDER_EMAIL;

    const testBucket = process.env.TEST_BUCKET;
    const productionBucket = process.env.PRODUCTION_BUCKET;

    if (!sender || !testBucket || !productionBucket) {
        throw new Error('Missing required environment variables.');
    }

    for (const record of event.Records) {

        const payload = getPayload(record.body);

        if (!payload.accessKey) {
            throw new Error('Missing accessKey in message payload.');
        }

        const companyId = getCompanyIdFromAccessKey(payload.accessKey);
        const bucket = payload.environment === 'production' ? productionBucket : testBucket;
        const key = `${companyId}/autorizados/${payload.accessKey}.xml`;



        const xmlBody = await s3.send(new GetObjectCommand({
            Bucket: bucket,
            Key: key
        }));

        if (!xmlBody.Body) {
            throw new Error(`Empty S3 object body for ${bucket}/${key}.`);
        }

        const xmlBuffer = Buffer.from(await xmlBody.Body.transformToByteArray());
        const xmlBase64 = xmlBuffer.toString('base64');
        const boundary = `sri-notifier-${Date.now()}`;
        //TODO: Read recipient from xml file
        const recipient = 'nelson.trujillo.ec@gmail.com';
        const rawMessage = [
            `From: ${sender}`,
            `To: ${recipient}`,
            `Subject: ${payload.subject}`,
            'MIME-Version: 1.0',
            `Content-Type: multipart/mixed; boundary="${boundary}"`,
            '',
            `--${boundary}`,
            'Content-Type: text/plain; charset="UTF-8"',
            'Content-Transfer-Encoding: 7bit',
            '',
            payload.body,
            '',
            `--${boundary}`,
            `Content-Type: application/xml; name="${payload.accessKey}.xml"`,
            'Content-Transfer-Encoding: base64',
            `Content-Disposition: attachment; filename="${payload.accessKey}.xml"`,
            '',
            xmlBase64,
            '',
            `--${boundary}--`,
            ''
        ].join('\r\n');

        await ses.send(new SendRawEmailCommand({
            RawMessage: {
                Data: Buffer.from(rawMessage)
            }
        }));
    }
};
