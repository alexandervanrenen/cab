// Extract important metric from a snowflake json file as found in their query analyzer

const fs = require("fs");

let data = JSON.parse(fs.readFileSync("data.json"));

const operator_names = [
   "TableScan",
   "Filter",
   "JoinFilter",
   "Join",
   "Aggregate",
   "Sort",
   "SortWithLimit",
   "Result",
   "Projection",
   "ExternalScan",
   "TryDeduplicate",
   "Insert",
   "Delete",
   "WithClause",
   "WithReference",
   "UnionAll",
   "GroupingSets",
   "LocalStop",
   "WindowFunction",
   "Limit",
   "TopK",
   "CartesianJoin",
   "Generator",
]
const query_metric_names = [
   "Processing",
   "Remote Disk IO",
   "Local Disk IO",
   "Initialization",
   "Synchronization",
   "Network Communication",
]
const query_statistic_names = [
   "Number of rows deleted",
   "Scan progress",
   "Bytes scanned",
   "Percentage scanned from cache",
   "Bytes written",
   "Bytes sent over the network",
   "Partitions scanned",
   "Partitions total",
   "Bytes written to result",
   "Number of rows inserted",
   "External bytes scanned",
]

const aggregated_stats = {};
[...operator_names, ...query_metric_names, ...query_statistic_names].forEach(name => aggregated_stats[name] = 0);
let total_time = 0;
let step_count = 0;
let result_rows = 0;

data.data.steps.forEach(step => {
   let result_operator_id = null;

   // Per operator stats for this tep
   step.graphData.nodes.forEach((node) => {
      if (!operator_names.includes(node.name)) throw "Unknown Operator: " + node.name;
      aggregated_stats[node.name] += node.totalStats.value;
      if (node.name === "Result") {
         if (result_operator_id != null) throw "More than one result operator in a single step";
         result_operator_id = node.id;
      }
   })

   // Global stats for this step
   step_count++;
   total_time += step.timeInMs;
   step.graphData.global.waits.forEach(wait => {
      if (!query_metric_names.includes(wait.name)) throw "Unknown Metric: " + wait.name;
      aggregated_stats[wait.name] += wait.value;
   })
   foo = (stat) => {
      if (!query_statistic_names.includes(stat.name)) throw "Unknown Statistic: " + stat.name;
      aggregated_stats[stat.name] += stat.value;
   };
   step.graphData.global.statistics.DML?.forEach(foo);
   step.graphData.global.statistics.IO?.forEach(foo);
   step.graphData.global.statistics.Network?.forEach(foo);
   step.graphData.global.statistics.Pruning?.forEach(foo);

   // Retrieve number of result rows
   if (result_operator_id == null) throw "Step without a result operator";
   const result_edge = step.graphData.edges.find(edge => edge.dst === result_operator_id);
   result_rows = result_edge.rows;
})

// Calculate the percentages
const operators = operator_names.map(name => (aggregated_stats[name]));
const query_metrics = query_metric_names.map(name => (aggregated_stats[name]));
const query_statistics = query_statistic_names.map(name => (aggregated_stats[name]));

// Print them out
// console.log(["Time(ms)", "Steps", ...operator_names, ...query_metric_names, ...query_statistic_names, "Rows"].join(","));
console.log([total_time, step_count, ...operators, ...query_metrics, ...query_statistics, result_rows].join(","));
