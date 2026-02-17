import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SESClient, SendRawEmailCommand } from '@aws-sdk/client-ses';
import type { SQSHandler } from 'aws-lambda';
import { XMLParser } from 'fast-xml-parser';


type EmailPayload = {
    accessKey: string;
    environment?: 'test' | 'production';
};

const s3 = new S3Client({});
const ses = new SESClient({});


const getPayload = (body: string): Required<EmailPayload> => {
    try {
        const parsed = JSON.parse(body) as EmailPayload;
        return {
            accessKey: parsed.accessKey,
            environment: parsed.environment || 'test'
        };
    } catch {
        return {
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

const extractEmailFromXML = (xmlString: string): string => {
    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: "@_"
    });
    
    const parsed = parser.parse(xmlString);
    const infoAdicional = parsed?.factura?.infoAdicional;
    
    if (!infoAdicional?.campoAdicional) {
        throw new Error('No infoAdicional section found in XML');
    }
    
    const campos = Array.isArray(infoAdicional.campoAdicional) 
        ? infoAdicional.campoAdicional 
        : [infoAdicional.campoAdicional];
    
    const emailField = campos.find((campo: any) => campo['@_nombre'] === 'Email');
    
    if (!emailField || !emailField['#text']) {
        throw new Error('Email field not found in XML');
    }
    
    return emailField['#text'];
};

export const handler: SQSHandler = async (event) => {
    const sender = process.env.SENDER_EMAIL;

    const testBucket = process.env.TEST_BUCKET;
    const productionBucket = process.env.PRODUCTION_BUCKET;

    if (!sender || !testBucket || !productionBucket) {
        throw new Error('Missing required environment variables.');
    }

    const failures: Array<{ messageId: string; error: string }> = [];

    for (const record of event.Records) {
        try {
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
            const xmlString = xmlBuffer.toString('utf-8');
            const xmlBase64 = xmlBuffer.toString('base64');
            const boundary = `sri-notifier-${Date.now()}`;
            const recipient = extractEmailFromXML(xmlString);

            if (!recipient || recipient.trim() === '') {
                throw new Error('Recipient email not found in XML.');
            }

            const subjectEnv = payload.environment === 'production' ? 'PRODUCCIÓN' : 'PRUEBAS';
            const subject = `Factura autorizada - ${subjectEnv}`;

            const body = `Estimado cliente,\n\nAdjunto encontrará la factura autorizada correspondiente a la clave de acceso ${payload.accessKey}.\n\nSaludos cordiales.`;
            
            const rawMessage = [
                `From: ${sender}`,
                `To: ${recipient}`,
                `Subject: ${subject}`,
                'MIME-Version: 1.0',
                `Content-Type: multipart/mixed; boundary="${boundary}"`,
                '',
                `--${boundary}`,
                'Content-Type: text/plain; charset="UTF-8"',
                'Content-Transfer-Encoding: 7bit',
                '',
                body,
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

            console.log('Email sent successfully:', {
                messageId: record.messageId,
                recipient,
                accessKey: payload.accessKey
            });

        } catch (error) {
            const errorMessage = error instanceof Error ? error.message : String(error);
            
            console.error('Failed to process record:', {
                messageId: record.messageId,
                error: errorMessage,
                body: record.body
            });

            failures.push({
                messageId: record.messageId,
                error: errorMessage
            });
        }
    }

    // If any messages failed, report them and throw to trigger retry/DLQ
    if (failures.length > 0) {
        console.error(`Failed to process ${failures.length} message(s):`, failures);
        throw new Error(`Failed to process ${failures.length} out of ${event.Records.length} messages`);
    }
};
