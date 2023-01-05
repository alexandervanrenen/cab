import AWS from "aws-sdk";
import AthenaExpress from "athena-express";
import {Semaphore} from 'async-mutex';

const connection_options = {
   region: "eu-central-1",
   accessKeyId: process.env.S3_ACCESS_KEY,
   secretAccessKey: process.env.S3_SECRET_ACCESS_KEY,
};

export default class AthenaPool {
   static GetConfig() {
      return {
         name: "athena",
         connector: AthenaPool,
         query_template_path: "sql_athena",
      };
   }

   constructor(athena_pool_concurrency, skip_results) {
      this.semaphore = new Semaphore(athena_pool_concurrency);
      this.connection_options = connection_options;
      AWS.config.update(this.connection_options);
      this.used_connection_count = 0;

      const express_athena_options = {
         aws: AWS, // required
         s3: "s3://[TODO: specify respective s3 bucket]", // optional
         db: process.env.DATABASE, // optional
         // workgroup: "myWorkGroupName", // optional
         formatJson: true, // optional
         retry: 0, // optional
         getStats: true, // optional
         ignoreEmpty: true, // optional
         // encryption: { EncryptionOption: "SSE_KMS", KmsKey: process.env.kmskey}, // optional
         skipResults: skip_results, // optional
         // waitForResults: false, // optional
         // catalog: "hive" //optional
      };

      this.athena = new AthenaExpress(express_athena_options);
      Object.assign(express_athena_options, {skipResults: true});
      this.athena_no_res = new AthenaExpress(express_athena_options);
   }

   static _FillBinds(sql, binds) {
      if (binds == null) {
         return sql;
      }

      let result = sql;
      for (let i = binds.length - 1; i >= 0; i--) {
         if (binds[i] == null) {
            return result;
         }

         if (typeof (binds[i]) === "number") {
            result = result.replaceAll(":" + (i + 1), binds[i]);
         } else if (typeof (binds[i]) === "string") {
            result = result.replaceAll(":" + (i + 1), "'" + binds[i] + "'");
         } else {
            throw new Error("unknown type for binding");
         }
      }
      return result;
   }

   async RunPrintSync(sql, binds) {
      this.used_connection_count++;
      const bound_sql = AthenaPool._FillBinds(sql, binds);
      const [_value, release] = await this.semaphore.acquire();
      const start = Date.now();
      const res = await this.athena.query(bound_sql);
      const done = Date.now();
      console.log(res);
      this.used_connection_count--;
      release();
      return {rows: res.Items, time: done - start, cost: res.QueryCostInUSD, scanned: res.DataScannedInMB};
   }

   async RunSync(sql, binds) {
      this.used_connection_count++;
      const bound_sql = AthenaPool._FillBinds(sql, binds);
      const [_value, release] = await this.semaphore.acquire();
      const start = Date.now();
      const res = await this.athena.query(bound_sql);
      const done = Date.now();
      this.used_connection_count--;
      release();
      return {rows: res.Items, time: done - start, cost: res.QueryCostInUSD, scanned: res.DataScannedInMB};
   }

   async RunArraySync(sql, binds) {
      this.used_connection_count++;
      const [_value, release] = await this.semaphore.acquire();
      const result = [];
      for (let idx = 0; idx < sql.length; idx++) {
         const bound_sql = AthenaPool._FillBinds(sql[idx], binds);
         const start = Date.now();
         const res = await this.athena_no_res.query(bound_sql); // NOTE: very hacky, we only use this method for dml queries, API crashes when there is a query without a result
         const done = Date.now();
         result.push({rows: res.Items, time: done - start, cost: res.QueryCostInUSD, scanned: res.DataScannedInMB});
      }
      this.used_connection_count--;
      release();
      return result;
   }

   async Wait() {
      const sleep = () => new Promise((r) => setTimeout(r, 100));

      while (true) {
         if (this.used_connection_count === 0) {
            return;
         }
         await sleep();
      }
   }
}
