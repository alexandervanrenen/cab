import nReadlines from 'n-readlines';
import * as readline from 'node:readline/promises';
import {stdin as input, stdout as output} from 'node:process';
import {ParquetSchema, ParquetWriter} from 'parquets';
import exec from 'child_process';

export default class Common {
   static LoadDatabaseMetaInfo(path) {
      const databases = [];
      while (true) {
         try {
            const file = new nReadlines(path + "/query_stream_" + databases.length + ".json");
            let meta_str = "";
            let line;
            while (line = file.next()) {
               if (line.includes("queries")) { break; }
               meta_str += line;
            }
            databases.push(JSON.parse(meta_str.slice(0, -1) + "}"));
         } catch (e) { break; }
      }
      return databases;
   }

   static async ConfirmRun() {
      const rl = readline.createInterface({input, output});
      const answer = await rl.question("Are you sure, this will replace all existing tables [yes]/no: ");
      rl.close();
      if (answer !== 'yes' && answer !== 'y') {
         process.exit(0);
      }
   }

   static FormatChildOutput(program, str) {
      if (str.substr(-1) === '\n') {
         str = str.substr(0, str.length - 1);
      }
      str = str.replaceAll("\n", "\n" + program + ": ");
      return program + ": " + str;
   }

   static async RunSnowSqlCommand(args) {
      return new Promise((r) => {
         process.env.SNOWSQL_PWD = process.env.PASSWORD;
         process.env.SNOWSQL_ACCOUNT = process.env.ACCOUNT;
         process.env.SNOWSQL_USER = process.env.USER;
         process.env.SNOWSQL_WAREHOUSE = process.env.WAREHOUSE;

         const child = exec.spawn("snowsql", args);
         child.stdout.on('data', (data) => { console.log(Common.FormatChildOutput("snowsql", data.toString())); });
         child.stderr.on('data', (data) => { console.log(Common.FormatChildOutput("snowsql", data.toString())); });
         child.on('close', (code, signal) => {
            if (code !== 0) throw new Error("non zero exit code from dbgen");
            r();
         });
         child.on('error', (err) => { throw err; });
      });
   }

   static async CsvToParquet() {
      let schema = new ParquetSchema({
         ps_partkey: {type: 'INT64'},
         ps_suppkey: {type: 'INT64'},
         ps_availqty: {type: 'INT64'},
         ps_supplycost: {type: 'INT64'},
         ps_comment: {type: 'UTF8'},
      });

      let writer = await ParquetWriter.openFile(schema, 'fruits.parquet');

      try {
         let cnt = 0;
         const file = new nReadlines("gen_33867/partsupp.tbl");
         let line;
         while (line = file.next()) {
            cnt++;
            const row = line.toString().split('|');
            row.pop();
            console.log(row);
            await writer.appendRow({
                  ps_partkey: row[0],
                  ps_suppkey: row[1],
                  ps_availqty: row[2],
                  ps_supplycost: row[3],
                  ps_comment: row[4],
               }
            );
            if (cnt > 10) break;
         }
      } catch (e) {
         console.log(e);
      }
      await writer.close();
   }
}