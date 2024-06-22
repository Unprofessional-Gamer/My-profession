const https = require('https');
const xml2js = require('xml2js');
const { Storage } = require('@google-cloud/storage');
const base64 = require('base-64');
const { Buffer } = require('buffer');
const { createLogger, transports, format } = require('winston');

const downloadAndUploadToGCS = async () => {
  const logger = createLogger({
    level: 'info',
    format: format.combine(
      format.timestamp({
        format: 'YYYY-MM-DD HH:mm:ss'
      }),
      format.errors({ stack: true }),
      format.splat(),
      format.json()
    ),
    transports: [
      new transports.Console()
    ]
  });

  try {
    // Fetch the SOAP response
    const url = "https://soap.cap.co.uk/datadownload/datadownload_webservice.asmx/Stream_LatestPackage?SubscriberID=406&Password=lloyd406&ProductID=1324";
    const response = await fetch(url);
    const responseText = await response.text();
    logger.info('Fetched the response.... merging all the chunks');

    // Parse the XML response
    const parser = new xml2js.Parser({ explicitArray: false });
    const result = await parser.parseStringPromise(responseText);

    // Check if response has the necessary data
    const root = result['soap:Envelope']['soap:Body']['Stream_LatestPackageResponse']['Stream_LatestPackageResult'];
    if (!root) throw new Error("Invalid XML response structure");

    const fileName = root['name'];
    logger.info(`For filename: ${fileName}, chunks are being merged`);

    // Extract and decode chunks
    const chunks = root['Chunk'];
    const fileData = Buffer.concat(chunks.map(chunk => Buffer.from(base64.decode(chunk), 'binary')));

    logger.info("Chunks merged. Uploading to GCS bucket");

    // Initialize Google Cloud Storage client
    const storage = new Storage({ projectId: 'tnt-01-bld' });

    // Define your GCS bucket and destination file path
    const bucketName = 'your-bucket-name';
    const destinationBlobName = `thParty/MFVS/GFV/${fileName}`;
    const bucket = storage.bucket(bucketName);
    const blob = bucket.file(destinationBlobName);

    // Upload the file data to GCS with specific content type for ZIP
    await blob.save(fileData, { contentType: 'application/zip' });

    logger.info(`File '${fileName}' uploaded to GCS bucket '${bucketName}' at path '${destinationBlobName}' with content type 'application/zip'`);
  } catch (error) {
    logger.error(`An error occurred: ${error.message}`);
  }
};

downloadAndUploadToGCS();
