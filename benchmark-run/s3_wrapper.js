import AWS from 'aws-sdk';
import fs from 'fs';

const connection_options = {
   accessKeyId: process.env.S3_ACCESS_KEY || null,
   secretAccessKey: process.env.S3_SECRET_ACCESS_KEY || null,
};
const bucket = process.env.S3_BUCKET || null;

export default class S3Wrapper {
   constructor() {
      if (Object.keys(connection_options).some(key => connection_options[key] == null)) {
         console.log("connection_options: " + JSON.stringify(connection_options, null, 2));
         console.log("Please define all these options :)");
         process.exit(-1);
      }
      if (bucket == null) {
         console.log("s3 bucket is not specified, please set S3_BUCKET in environment.");
         process.exit(-1);
      }

      this.connection = new AWS.S3(connection_options);
      this.connection_options = connection_options;
      this.bucket = bucket;
      console.log("done")
   }

   _GetS3Path(database_id, table_name, step) {
      return "csv/database_" + database_id + "/" + table_name + "_" + step + "/";
   }

   _GetS3FileName(database_id, table_name, step) {
      return this._GetS3Path(database_id, table_name, step) + "chunk" + ".csv.zst";
   }

   _GetS3AccessPath(database_id, table_name, step) {
      return "'s3://" + this.bucket + "/" + this._GetS3FileName(database_id, table_name, step) + "'";
   }

   _GetS3AccessPathFolder(database_id, table_name, step) {
      return "'s3://" + this.bucket + "/" + this._GetS3Path(database_id, table_name, step) + "'";
   }

   async put(key, file) {
      const file_content = fs.readFileSync(file);

      // Setting up S3 upload parameters
      const params = {
         Bucket: bucket,
         Key: key,
         Body: file_content
      };

      // Uploading files to the bucket
      return new Promise((r) => {
         this.connection.upload(params, function (err, data) {
            if (err) {
               throw err;
            }
            console.log(`File uploaded successfully. ${data.Location}`);
            r();
         });
      });
   }
}