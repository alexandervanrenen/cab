import SnowflakePool from './snowflake_pool.js';
import Common from './common.js';
import chalk from 'chalk';
import fs from "fs";

const gb_per_chunk = 1;
const snowflake_pool_concurrency = 5;

class LoadSetupManager {
   constructor() {
      this.query_streams = [];
      this.snowflake = new SnowflakePool(snowflake_pool_concurrency, false);
   }

   // Read query_streams from disk and gather meta info for all databases
   LoadDatabases(path) {
      this.query_streams = Common.LoadDatabaseMetaInfo(path);
      if (this.query_streams.length === 0) {
         console.log(chalk.red("No query streams were found, make sure the first one is query_stream_0.json"));
         process.exit(0);
      }
   }

   // Create tables for all scale_factors
   async CreateDataTablesSnowflake() {
      console.log(chalk.cyan("\nCreating snowflake tables ..."));
      for (const query_stream of this.query_streams) {
         this.snowflake.RunPrintSync("create or replace TABLE REGION_" + query_stream.database_id + " ( R_REGIONKEY NUMBER(38,0), R_NAME VARCHAR(25), R_COMMENT VARCHAR(152) );");
         this.snowflake.RunPrintSync("create or replace TABLE NATION_" + query_stream.database_id + " ( N_NATIONKEY NUMBER(38,0), N_NAME VARCHAR(25), N_REGIONKEY NUMBER(38,0), N_COMMENT VARCHAR(152) );");
         this.snowflake.RunPrintSync("create or replace TABLE CUSTOMER_" + query_stream.database_id + " ( C_CUSTKEY NUMBER(38,0), C_NAME VARCHAR(25), C_ADDRESS VARCHAR(40), C_NATIONKEY NUMBER(38,0), C_PHONE VARCHAR(15), C_ACCTBAL NUMBER(12,2), C_MKTSEGMENT VARCHAR(10), C_COMMENT VARCHAR(117));");
         this.snowflake.RunPrintSync("create or replace TABLE LINEITEM_" + query_stream.database_id + " ( L_ORDERKEY NUMBER(38,0), L_PARTKEY NUMBER(38,0), L_SUPPKEY NUMBER(38,0), L_LINENUMBER NUMBER(38,0), L_QUANTITY NUMBER(12,2), L_EXTENDEDPRICE NUMBER(12,2), L_DISCOUNT NUMBER(12,2), L_TAX NUMBER(12,2), L_RETURNFLAG VARCHAR(1), L_LINESTATUS VARCHAR(1), L_SHIPDATE DATE, L_COMMITDATE DATE, L_RECEIPTDATE DATE, L_SHIPINSTRUCT VARCHAR(25), L_SHIPMODE VARCHAR(10), L_COMMENT VARCHAR(44));");
         this.snowflake.RunPrintSync("create or replace TABLE ORDERS_" + query_stream.database_id + " ( O_ORDERKEY NUMBER(38,0), O_CUSTKEY NUMBER(38,0), O_ORDERSTATUS VARCHAR(1), O_TOTALPRICE NUMBER(12,2), O_ORDERDATE DATE, O_ORDERPRIORITY VARCHAR(15), O_CLERK VARCHAR(15), O_SHIPPRIORITY NUMBER(38,0), O_COMMENT VARCHAR(79));");
         this.snowflake.RunPrintSync("create or replace TABLE PART_" + query_stream.database_id + " ( P_PARTKEY NUMBER(38,0), P_NAME VARCHAR(55), P_MFGR VARCHAR(25), P_BRAND VARCHAR(10), P_TYPE VARCHAR(25), P_SIZE NUMBER(38,0), P_CONTAINER VARCHAR(10), P_RETAILPRICE NUMBER(12,2), P_COMMENT VARCHAR(23));");
         this.snowflake.RunPrintSync("create or replace TABLE PARTSUPP_" + query_stream.database_id + " ( PS_PARTKEY NUMBER(38,0), PS_SUPPKEY NUMBER(38,0), PS_AVAILQTY NUMBER(38,0), PS_SUPPLYCOST NUMBER(12,2), PS_COMMENT VARCHAR(199) );");
         this.snowflake.RunPrintSync("create or replace TABLE SUPPLIER_" + query_stream.database_id + " ( S_SUPPKEY NUMBER(38,0), S_NAME VARCHAR(25), S_ADDRESS VARCHAR(40), S_NATIONKEY NUMBER(38,0), S_PHONE VARCHAR(15), S_ACCTBAL NUMBER(12,2), S_COMMENT VARCHAR(101) );");
         await this.snowflake.Wait();
      }
   }

   async CreateJobTables() {
      console.log(chalk.cyan("\nCreating load jobs ..."));

      await this.snowflake.RunPrintSync("create or replace table Jobs(job_id int, database_id int, scale_factor int, table_name string, chunk_count int, step int, status string);");
      await this.snowflake.Wait();
   }

   // Inserts "load-tasks" into the Jobs table. Each task populates a chunk of a table
   async CreateLoadJobs() {
      let job_id = 1;

      const split_table_into_chunks = (database_id, scale_factor, gb_per_scale_factor, relation_name) => {
         const relation_gb = (scale_factor * gb_per_scale_factor);
         const relation_chunks = Math.ceil(relation_gb / gb_per_chunk);
         return Array.from(Array(relation_chunks).keys()).map(k => [job_id++, database_id, scale_factor, relation_name, relation_chunks, k + 1, 'open']);
      }

      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'region', 1, 1, 'open']));
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => [job_id++, db.database_id, db.scale_factor, 'nation', 1, 1, 'open']));
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'customer')).flat());
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.725, 'lineitem')).flat());
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.164, 'orders')).flat());
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.023, 'part')).flat());
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.113, 'partsupp')).flat());
      this.snowflake.RunPrintSync("insert into Jobs values(?, ?, ?, ?, ?, ?, ?);", this.query_streams.map(db => split_table_into_chunks(db.database_id, db.scale_factor, 0.001, 'supplier')).flat());
      await this.snowflake.Wait();
   }
}

async function main() {
   await Common.ConfirmRun();

   const setup_manager = new LoadSetupManager();
   setup_manager.LoadDatabases("query_streams");
   await setup_manager.CreateDataTablesSnowflake();
   await setup_manager.CreateJobTables();
   await setup_manager.CreateLoadJobs();

   console.log(chalk.cyan("\nNormal program exit: setup " + setup_manager.query_streams.length + " databases :)"));
}

main();
