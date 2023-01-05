import SnowflakePool from './snowflake_pool.js';
import AthenaPool from './athena_pool.js';
import BigQueryPool from './bigquery_pool.js';
import Common from './common.js';
import chalk from 'chalk';

const pool_concurrency = 1;

class LoadValidator {
   constructor(database_config) {
      this.databases = [];
      this.database_config = database_config;
      this.database_connection = new database_config.connector(pool_concurrency, false);
      this.all_errors = [];
   }

   _PrintCleanupMessage(relation, database_id, expected, actual) {
      const relation_name = relation + "_" + database_id;
      console.log(chalk.red("The '" + relation_name + "' table has " + actual + " tuples, instead of the expected " + expected + "."));
      console.log(chalk.red("Restart loading for this relation by using:"));
      console.log(chalk.red("   delete from " + relation_name + ";"));
      console.log(chalk.red("   update Jobs set status = 'open' where table_name = '" + relation + "' and database_id = " + database_id + ";"));
      this.all_errors.push("delete from " + relation_name + ";")
      this.all_errors.push("update Jobs set status = 'open' where table_name = '" + relation + "' and database_id = " + database_id + ";");
   }

   async _ValidateCount(relation, database_id, expected) {
      const relation_name = relation + "_" + database_id;
      const res = await this.database_connection.RunPrintSync("select count(*) as CNT from " + relation_name + ";");
      if (Number(res.rows[0].CNT) !== expected) {
         this._PrintCleanupMessage(relation, database_id, expected, res.rows[0].CNT);
      }
   }

   async _ValidateCountLineItem(relation, database_id, expected) {
      const relation_name = relation + "_" + database_id;
      const res = await this.database_connection.RunPrintSync("select count(*) as CNT from " + relation_name + ";");
      if (Number(res.rows[0].CNT) < expected * 0.99 || res.rows[0].CNT > expected * 1.01) {
         this._PrintCleanupMessage(relation, database_id, expected, res.rows[0].CNT);
      }
   }

   async ValidateTableCardinalities(path) {
      this.databases = Common.LoadDatabaseMetaInfo(path);

      for (const database of this.databases) {
         await this._ValidateCount('region', database.database_id, 5);
         await this._ValidateCount('nation', database.database_id, 25);
         await this._ValidateCount('customer', database.database_id, 150000 * database.scale_factor);
         await this._ValidateCount('orders', database.database_id, 1500000 * database.scale_factor);
         await this._ValidateCount('part', database.database_id, 200000 * database.scale_factor);
         await this._ValidateCount('partsupp', database.database_id, 800000 * database.scale_factor);
         await this._ValidateCount('supplier', database.database_id, 10000 * database.scale_factor);
         await this._ValidateCountLineItem('lineitem', database.database_id, 6000000 * database.scale_factor);
      }
   }

   async ValidateTableKeys(path) {
      this.databases = Common.LoadDatabaseMetaInfo(path);

      for (const database of this.databases) {
         console.log("select distinct mod(o_orderkey, 32) from orders_" + database.database_id);
         const order_res = await this.database_connection.RunPrintSync("select distinct mod(o_orderkey, 32) from orders_" + database.database_id);
         if (order_res.rows.length !== 8) {
            console.log(chalk.red("Wrong keys in orders_" + database.database_id + ". Use the following to fix:"));
            console.log(chalk.red("update orders_" + database.database_id + " set o_orderkey = floor(o_orderkey / 32) * 32 + mod(o_orderkey, 8);\n"))
            this.all_errors.push("update orders_" + database.database_id + " set o_orderkey = floor(o_orderkey / 32) * 32 + mod(o_orderkey, 8);")
         }
         const lineitem_res = await this.database_connection.RunPrintSync("select distinct mod(l_orderkey, 32) from lineitem_" + database.database_id);
         if (lineitem_res.rows.length !== 8) {
            console.log(chalk.red("Wrong keys in lineitem_" + database.database_id + ". Use the following to fix:"));
            console.log(chalk.red("update lineitem_" + database.database_id + " set l_orderkey = floor(l_orderkey / 32) * 32 + mod(l_orderkey, 8);\n"))
            this.all_errors.push("update lineitem_" + database.database_id + " set l_orderkey = floor(l_orderkey / 32) * 32 + mod(l_orderkey, 8);")
         }
      }
   }
}

async function main() {
   const worker = new LoadValidator(BigQueryPool.GetConfig());
   await worker.ValidateTableCardinalities("query_streams");
   await worker.ValidateTableKeys("query_streams");

   console.log(chalk.cyan("\nNormal program exit: done :)"));

   console.log(worker.all_errors.join("\n"));
}

main();
