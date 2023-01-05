import BigQuery from '@google-cloud/bigquery';
import {Semaphore} from 'async-mutex';

const connection_options = {
   dataset_name: process.env.GOOGLE_DATASET || null,
};

export default class BigQueryPool {
   static GetConfig() {
      return {
         name: "bigquery",
         connector: BigQueryPool,
         query_template_path: "sql_big_query",
      };
   }

   constructor(max_connection_count, skip_results) {
      if (Object.keys(connection_options).some(key => connection_options[key] == null)) {
         console.log("connection_options: " + JSON.stringify(connection_options, null, 2));
         console.log("Please define all these options :)");
         process.exit(-1);
      }
      this.connection_options = connection_options;

      this.used_connection_count = 0;
      this.skip_results = skip_results;
      this.pool = new BigQuery.BigQuery();
      this.semaphore = new Semaphore(max_connection_count);
   }

   async RunPrintSync(sql, binds) {
      return await this.RunSync(sql, binds, true)
   }

   async RunSync(sql, binds, verbose) {
      this.used_connection_count++;
      const bound_sql = BigQueryPool._FillBinds(sql, binds);
      const [_value, release] = await this.semaphore.acquire();
      const start = Date.now();
      const options = {query: bound_sql, location: "europe-west3", defaultDataset: {datasetId: this.connection_options.dataset_name, projectId: "fau-cab"}};
      const [job] = await this.pool.createQueryJob(options);
      const [rows] = await job.getQueryResults();
      if (verbose) {
         console.log(rows);
      }
      const done = Date.now();
      this.used_connection_count--;
      release();
      return {rows: rows, time: done - start};
   }

   async RunArraySync(sql, binds) {
      if (sql.length > 1) {
         throw "big query should not require this array hack, because it can run multiple statements in one request";
      }
      return await this.RunSync(sql[0], binds);
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
}