import snowflake from 'snowflake-sdk';

const connection_options = {
   account: process.env.ACCOUNT || null,
   username: process.env.USER || null,
   password: process.env.PASSWORD || null,
   warehouse: process.env.WAREHOUSE || null,
   database: process.env.DATABASE || null,
   schema: process.env.SCHEMA || null,
   application: process.env.WAREHOUSE || "Load_Setup",
};

export default class SnowflakePool {
   static GetConfig() {
      return {
         name: "snowflake",
         connector: SnowflakePool,
         query_template_path: "sql_snowflake",
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
      this.max_connection_count = max_connection_count;
      this.skip_results = skip_results;

      this.pool = snowflake.createPool(
         connection_options,
         {min: 0, max: max_connection_count,}
      );
   }

   async _ExecuteOnConnectionSync(connection, sql, binds) {
      return new Promise(async r => {
         connection.execute({
            sqlText: sql,
            binds: binds,
            streamResult: this.skip_results, // streaming == no rows send initially
            complete: function (err, stmt, rows) {
               if (err) { throw err; }
               r(rows);
            }
         });
      });
   }

   // Waits for a free connection, then starts the task and returns with a promise on the results
   async RunSync(sql, binds) {
      this.used_connection_count++;

      // Need two promises here, this one blocks on the result; the one in _ExecuteOnConnectionSync blocks the connection
      return new Promise(async r => {
         await this.pool.use(async (connection) => {
            const start = Date.now();
            const rows = await this._ExecuteOnConnectionSync(connection, sql, binds);
            const done = Date.now();
            this.used_connection_count--;
            r({rows: rows, time: done - start});
         });
      });
   }

   async RunPrintSync(sql, binds) {
      const res = await this.RunSync(sql, binds);
      console.log(res.rows);
      return res;
   }

   // Waits for a free connection, then starts all the tasks and returns with a promise on all the results
   // Note: the same bind is used for each query
   async RunArraySync(sql, binds) {
      this.used_connection_count++;

      // Need two promises here, this one blocks on the result; the one in _ExecuteOnConnectionSync blocks the connection
      return new Promise(async r => {
         await this.pool.use(async (connection) => {
            const start = Date.now();
            const rows = [];
            for (let idx = 0; idx < sql.length; idx++) {
               rows.push(await this._ExecuteOnConnectionSync(connection, sql[idx], binds));
            }
            const done = Date.now();
            this.used_connection_count--;
            r({rows: rows, time: done - start});
         });
      });
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