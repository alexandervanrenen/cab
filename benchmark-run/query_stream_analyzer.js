import SnowflakePool from './snowflake_pool.js';
import fs from "fs";
import chalk from 'chalk';
import table from 'table';

class QueryStreamAnalyzer {
   constructor() {
      this.query_streams = [];
      for (let i = 0; i <= 19; i++) {
         const data = fs.readFileSync("query_streams/query_stream_" + i + ".json");
         this.query_streams[i] = JSON.parse(data.toString());
      }
   }
}

async function main() {
   const executor = new QueryStreamAnalyzer();
   const data = [];
   data.push(["query_stream", "pattern", "scale_factor", "query count", "last query start [min]", "cpu time [h]"]);
   executor.query_streams.forEach((qs, idx) => {
      const row = [];
      row.push(idx);
      row.push(qs.pattern_id);
      row.push(qs.scale_factor);
      row.push(qs.query_count);
      row.push(Math.round(qs.queries[qs.queries.length - 1].start / 60e3));
      row.push((qs.cpu_time / 3600e6).toLocaleString('en-US', {minimumIntegerDigits: 1, minimumFractionDigits: 1, maximumFractionDigits: 1, useGrouping: false}));
      data.push(row);
   });

   const config = {
      drawHorizontalLine: (lineIndex, rowCount) => {
         return lineIndex === 0 || lineIndex === 1 || lineIndex === rowCount;
      },
      columns: [
         {alignment: 'left'},
         {alignment: 'right'},
         {alignment: 'right'},
         {alignment: 'right'},
         {alignment: 'right'},
         {alignment: 'right'},
      ],
        // border: {
        //   bodyLeft: ``,
        //   bodyRight: ``,
        //   bodyJoin: `,`,
        // }
   }
   let x = table.table(data, config);
   console.log(x)
}

main();
