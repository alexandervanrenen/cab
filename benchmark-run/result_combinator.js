import SnowflakePool from './snowflake_pool.js';
import AthenaPool from './athena_pool.js';
import fs from "fs";
import chalk from 'chalk';
import table from 'table';
import RJSON from 'relaxed-json';

class QueryStreamAnalyzer {
   constructor() {
      this.query_streams = [];
      let i = 0;
      while (true) {
         try {
            const data = fs.readFileSync("../results/big_query_1h_1tb/executor_" + i + ".log").toString();
            this.query_streams.push(data);
            i++;
         } catch (e) { break; }
      }
   }

   PrintAllCsv() {
      this.query_streams.forEach((qs, idx) => {
         const start = qs.indexOf("-- START CSV --") + "-- START CSV --".length;
         const stop = qs.indexOf("-- STOP CSV --");
         let csv_section = qs.substr(start, stop - start);
         csv_section = csv_section.split("\n").filter(line => line.length > 0); // remove blank lines
         const header = csv_section.splice(0, 1); // remove header
         if (idx === 0) {
            console.log("type,runtime,warehouse_size," + header[0]);
         }
         csv_section = csv_section.map(line => "big-query,3600,0," + line);
         console.log(csv_section.join("\n"));
      });
   }
}

const executor = new QueryStreamAnalyzer();
executor.PrintAllCsv();
