import { S3Client, GetObjectCommand } from '@aws-sdk/client-s3';
import { SESClient, SendRawEmailCommand } from '@aws-sdk/client-ses';
import type { SQSHandler, SQSRecord } from 'aws-lambda';
import { XMLParser } from 'fast-xml-parser';
import { PDFDocument, StandardFonts, rgb } from 'pdf-lib';


type EmailPayload = {
    eventType: string;
    status: string;
    accessKey: string;
    environment?: 'test' | 'production';
    timestamp: string;
};

const s3 = new S3Client({});
const ses = new SESClient({});

const getObjectBuffer = async (bucket: string, key: string): Promise<Buffer> => {
    const result = await s3.send(new GetObjectCommand({
        Bucket: bucket,
        Key: key
    }));

    if (!result.Body) {
        throw new Error(`Empty S3 object body for ${bucket}/${key}.`);
    }

    return Buffer.from(await result.Body.transformToByteArray());
};

const wrapText = (text: string, maxChars: number): string[] => {
    const normalized = text.replaceAll(/\s+/g, ' ').trim();
    if (!normalized) {
        return [''];
    }

    const words = normalized.split(' ');
    const lines: string[] = [];
    let current = '';

    for (const word of words) {
        const candidate = current ? `${current} ${word}` : word;

        if (candidate.length <= maxChars) {
            current = candidate;
        } else {
            if (current) {
                lines.push(current);
            }
            current = word;
        }
    }

    if (current) {
        lines.push(current);
    }

    return lines;
};

const generatePdfFromXml = async (xmlString: string, payload: Required<EmailPayload>): Promise<Buffer> => {
    const parser = new XMLParser({
        ignoreAttributes: false,
        attributeNamePrefix: "@_"
    });

    let parsed: any = {};
    try {
        parsed = parser.parse(xmlString);
    } catch {
        parsed = {};
    }

    const factura = parsed?.factura ?? {};
    const infoTributaria = factura?.infoTributaria ?? {};
    const infoFactura = factura?.infoFactura ?? {};
    const detallesRaw = factura?.detalles?.detalle;
    const detalles: any[] = [];

    if (Array.isArray(detallesRaw)) {
        detalles.push(...detallesRaw);
    } else if (detallesRaw) {
        detalles.push(detallesRaw);
    }

    const pdfDoc = await PDFDocument.create();
    const regularFont = await pdfDoc.embedFont(StandardFonts.Helvetica);
    const boldFont = await pdfDoc.embedFont(StandardFonts.HelveticaBold);
    const monoFont = await pdfDoc.embedFont(StandardFonts.Courier);

    const pageWidth = 595;
    const pageHeight = 842;
    const margin = 40;
    const contentWidth = pageWidth - (margin * 2);

    let page = pdfDoc.addPage([pageWidth, pageHeight]);
    let y = pageHeight - margin;

    const sectionTitle = (title: string): void => {
        page.drawRectangle({
            x: margin,
            y: y - 14,
            width: contentWidth,
            height: 15,
            color: rgb(0.92, 0.95, 1)
        });
        page.drawText(title, {
            x: margin + 6,
            y: y - 10,
            size: 10,
            font: boldFont,
            color: rgb(0.1, 0.2, 0.5)
        });
        y -= 20;
    };

    const fieldLine = (label: string, value: string): void => {
        page.drawText(`${label}:`, { x: margin, y, size: 9, font: boldFont });
        page.drawText(value || 'N/D', { x: margin + 125, y, size: 9, font: regularFont });
        y -= 12;
    };

    page.drawText('FACTURA ELECTRÓNICA AUTORIZADA', { x: margin, y, size: 16, font: boldFont });
    y -= 20;
    fieldLine('Clave de acceso', payload.accessKey);
    fieldLine('Ambiente', payload.environment === 'production' ? 'PRODUCCIÓN' : 'PRUEBAS');
    fieldLine('Generado', payload.timestamp);
    y -= 8;

    sectionTitle('Emisor');
    fieldLine('Razón social', String(infoTributaria?.razonSocial ?? 'N/D'));
    fieldLine('Nombre comercial', String(infoTributaria?.nombreComercial ?? 'N/D'));
    fieldLine('RUC', String(infoTributaria?.ruc ?? 'N/D'));
    fieldLine('Estab/PtoEmi/Secuencial', `${String(infoTributaria?.estab ?? '---')}-${String(infoTributaria?.ptoEmi ?? '---')}-${String(infoTributaria?.secuencial ?? '---')}`);

    y -= 8;
    sectionTitle('Comprador y Totales');
    fieldLine('Razón social comprador', String(infoFactura?.razonSocialComprador ?? 'N/D'));
    fieldLine('Identificación comprador', String(infoFactura?.identificacionComprador ?? 'N/D'));
    fieldLine('Fecha emisión', String(infoFactura?.fechaEmision ?? 'N/D'));
    fieldLine('Moneda', String(infoFactura?.moneda ?? 'DOLAR'));
    fieldLine('Subtotal', String(infoFactura?.totalSinImpuestos ?? '0.00'));
    fieldLine('Total', String(infoFactura?.importeTotal ?? '0.00'));

    y -= 8;
    sectionTitle('Detalle (primeros ítems)');
    const detalleLines = detalles.length > 0
        ? detalles.slice(0, 10).map((detalle: any) => {
            const descripcion = String(detalle?.descripcion ?? 'Sin descripción');
            const cantidad = String(detalle?.cantidad ?? '0');
            const unitario = String(detalle?.precioUnitario ?? '0.00');
            const total = String(detalle?.precioTotalSinImpuesto ?? '0.00');
            return `• ${descripcion} | Cant: ${cantidad} | Unit: ${unitario} | Total: ${total}`;
        })
        : ['• No disponible en XML'];

    for (const line of detalleLines) {
        for (const wrapped of wrapText(line, 96)) {
            page.drawText(wrapped, { x: margin, y, size: 8.5, font: regularFont });
            y -= 10;
        }
    }

    page = pdfDoc.addPage([pageWidth, pageHeight]);
    y = pageHeight - margin;

    page.drawText('ANEXO: XML ORIGINAL', {
        x: margin,
        y,
        size: 12,
        font: boldFont
    });
    y -= 18;

    const xmlLines = xmlString
        .replaceAll('\r\n', '\n')
        .split('\n')
        .flatMap((line) => wrapText(line, 106));

    for (const line of xmlLines) {
        if (y < margin + 12) {
            page = pdfDoc.addPage([pageWidth, pageHeight]);
            y = pageHeight - margin;
        }

        page.drawText(line, {
            x: margin,
            y,
            size: 8,
            font: monoFont,
            color: rgb(0.2, 0.2, 0.2)
        });

        y -= 10;
    }

    const pdfBytes = await pdfDoc.save();
    return Buffer.from(pdfBytes);
};

const processRecordEmail = async (
    record: SQSRecord,
    sender: string,
    testBucket: string,
    productionBucket: string
): Promise<void> => {
    const payload = getPayload(record.body);

    if (!payload.accessKey) {
        throw new Error('Missing accessKey in message payload.');
    }

    const companyId = getCompanyIdFromAccessKey(payload.accessKey);
    const bucket = payload.environment === 'production' ? productionBucket : testBucket;
    const xmlKey = `${companyId}/autorizados/${payload.accessKey}_aut.xml`;

    const xmlBuffer = await getObjectBuffer(bucket, xmlKey);

    const xmlString = xmlBuffer.toString('utf-8');
    const pdfBuffer = await generatePdfFromXml(xmlString, payload);
    const xmlBase64 = xmlBuffer.toString('base64');
    const pdfBase64 = pdfBuffer.toString('base64');
    const boundary = `sri-notifier-${Date.now()}`;
    const recipient = extractEmailFromXML(xmlString);

    if (!recipient || recipient.trim() === '') {
        throw new Error('Recipient email not found in XML.');
    }

    const subjectEnv = payload.environment === 'production' ? 'PRODUCCIÓN' : 'PRUEBAS';
    const subject = `Factura autorizada - ${subjectEnv}`;

    const body = `Estimado cliente,\n\nAdjunto encontrará la factura autorizada correspondiente a la clave de acceso ${payload.accessKey}.\n\nSaludos cordiales.`;

    const rawMessageParts = [
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
        `--${boundary}`,
        `Content-Type: application/pdf; name="${payload.accessKey}.pdf"`,
        'Content-Transfer-Encoding: base64',
        `Content-Disposition: attachment; filename="${payload.accessKey}.pdf"`,
        '',
        pdfBase64,
        '',
        `--${boundary}--`,
        ''
    ];

    const rawMessage = rawMessageParts.join('\r\n');

    await ses.send(new SendRawEmailCommand({
        RawMessage: {
            Data: Buffer.from(rawMessage)
        }
    }));

    console.log('Email sent successfully:', {
        messageId: record.messageId,
        recipient,
        accessKey: payload.accessKey,
        pdfAttached: true
    });
};


const getPayload = (body: string): Required<EmailPayload> => {
    try {
        const parsed = JSON.parse(body) as EmailPayload;
        return {
            accessKey: parsed.accessKey,
            environment: parsed.environment || 'test',
            eventType: parsed.eventType,
            status: parsed.status,
            timestamp: parsed.timestamp
        };
    } catch {
        return {
            accessKey: '',
            environment: 'test',
            eventType: '',
            status: '',
            timestamp: ''

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

    if (!emailField?.['#text']) {
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
            await processRecordEmail(record, sender, testBucket, productionBucket);

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
