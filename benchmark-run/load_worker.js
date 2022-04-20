import SnowflakePool from './snowflake_pool.js';
import S3Wrapper from './s3_wrapper.js';
import Common from './common.js';
import fs from "fs";
import exec from 'child_process';
import chalk from 'chalk';

const snowflake_pool_concurrency = 1;
const dbgen_file = "dbgen";
const dbgen_folder = "tpch-dbgen";
const dbgen_path = dbgen_folder + "/" + dbgen_file;
const csv_directory = "gen_" + process.pid;

class LoadWorker {
   constructor() {
      this.databases = [];
      this.snowflake = new SnowflakePool(snowflake_pool_concurrency, false);
      this.s3_wrapper = new S3Wrapper();
      this.stats = {
         compress_total: 0,
         dbgen_total: 0,
         upload_total: 0,
         copy_snowflake_total: 0,
         pipelined_upload_total: 0,
         compress: [],
         dbgen: [],
         upload: [],
         copy_snowflake: [],
         pipelined_upload: [],
      };
   }

   async GetNextJob() {
      console.log(chalk.cyan("\nGetting next job ..."));
      while (true) {
         const jobs = await this.snowflake.RunSync("select * from jobs where status = 'open' limit 1");
         if (jobs.rows.length === 0) {
            console.log("No more open jobs -> done");
            return null
         }
         const job = jobs.rows[0];

         const update_res = await this.snowflake.RunSync("update jobs set status = 'running' where job_id = ? and status = 'open'", [job.JOB_ID]);
         const updated_rows = update_res.rows[0]['number of rows updated'];
         if (updated_rows != null && updated_rows === 1) {
            console.log(job);
            return job;
         }
      }
   }

   async _EnsureThatDatabaseGeneratorIsAvailable() {
      return new Promise((r) => {
         fs.access(dbgen_path, fs.constants.X_OK, (err) => {
            if (err) {
               const error_message = "Can not execute '" + dbgen_path + "'.\n" +
                  "Make sure the tpc-h dbgen tool is installed in '" + dbgen_folder + "'.\n" +
                  "If it is located at a different location, you can adjust the `dbgen_folder` in this script.\n" +
                  "Otherwise download and compile it: https://github.com/electrum/tpch-dbgen";
               console.log(chalk.red(error_message));
               throw new Error(error_message);
            }
            r();
         });
      });
   }

   _TableNameToDbGenTableCode(table_name) {
      switch (table_name) {
         case "customer": { return "c"; }
         case "lineitem": { return "L"; }
         case "nation": { return "n"; }
         case "orders": { return "O"; }
         case "part": { return "P"; }
         case "region": { return "r"; }
         case "supplier": { return "s"; }
         case "partsupp": { return "S"; }
      }
   }

   async _RunCommand(command, args) {
      console.log(command + " " + args.join(" "));
      const start = Date.now();
      return new Promise((r) => {
         const child = exec.spawn(command, args);
         child.stdout.on('data', (data) => { console.log(Common.FormatChildOutput(command, data.toString())); });
         child.stderr.on('data', (data) => { console.log(Common.FormatChildOutput(command, data.toString())); });
         child.on('close', (code, signal) => {
            if (code !== 0) throw new Error("non zero exit code from " + command);
            r((Date.now() - start));
         });
         child.on('error', (err) => { throw err; });
      });
   }

   async CreateCsvFile(job) {
      const start = Date.now();

      // Check that dbgen is fine
      await this._EnsureThatDatabaseGeneratorIsAvailable(dbgen_path);
      if (!fs.existsSync(csv_directory)) {
         fs.mkdirSync(csv_directory);
      }

      // Create
      const args = ["-s", job.SCALE_FACTOR, "-T", this._TableNameToDbGenTableCode(job.TABLE_NAME), "-f", "-b", dbgen_folder + "/dists.dss"];
      if (job.CHUNK_COUNT > 1) {
         args.push("-C", job.CHUNK_COUNT, "-S", job.STEP);
      }
      console.log(chalk.cyan("\nGenerating table chunk ..."));
      process.env.DSS_PATH = csv_directory;
      await this._RunCommand(dbgen_path, args);
      this.stats.dbgen_total += (Date.now() - start);
      this.stats.dbgen.push({time: Date.now() - start, table_name: job.TABLE_NAME});

      // Set permissions, because sometimes they are not correct :/
      await this._RunCommand("chmod", ["u+rw", this._GetGeneratedFileName(job)]);
   }

   _GetGeneratedFileName(job) {
      return process.env.PWD + "/" + csv_directory + "/" + job.TABLE_NAME + ".tbl" + (job.CHUNK_COUNT > 1 ? "." + job.STEP : "");
   }

   async CompressCsvFile(job) {
      const generated_file = this._GetGeneratedFileName(job);
      const start = Date.now();
      const args = ["--quiet", generated_file];

      console.log(chalk.cyan("\nCompressing table chunk ..."));
      console.log("zstd " + args.join(" "));
      await this._RunCommand("zstd", args);
      this.stats.compress_total += (Date.now() - start);
      this.stats.compress.push({time: Date.now() - start, table_name: job.TABLE_NAME});
   }

   _GetCompressedFileName(job) {
      return this._GetGeneratedFileName(job) + ".zst";
   }

   async UploadCsvFileS3(job) {
      const start = Date.now();
      const generated_file = this._GetCompressedFileName(job);
      const s3_file = this.s3_wrapper._GetS3FileName(job.DATABASE_ID, job.TABLE_NAME, job.STEP);
      console.log(chalk.cyan("\nUpload csv file ..."));
      console.log(generated_file + " -> " + s3_file);
      await this.s3_wrapper.put(s3_file, generated_file);
      console.log("success.");
      this.stats.upload_total += (Date.now() - start);
      this.stats.upload.push({time: Date.now() - start, table_name: job.TABLE_NAME});
   }

   async DoPipelinedUpload(job) {
      const start = Date.now();

      const dbgen_args = ["-s", job.SCALE_FACTOR, "-T", this._TableNameToDbGenTableCode(job.TABLE_NAME), "-f", "-b", dbgen_folder + "/dists.dss"];
      if (job.CHUNK_COUNT > 1) {
         dbgen_args.push("-C", job.CHUNK_COUNT, "-S", job.STEP);
      }

      // Generate script
      const script_file_content = "" +
         "export AWS_ACCESS_KEY_ID=" + this.s3_wrapper.connection_options.accessKeyId + "\n" +
         "export AWS_SECRET_ACCESS_KEY=" + this.s3_wrapper.connection_options.secretAccessKey + "\n" +
         "export DSS_PATH=" + csv_directory + "\n" +
         "mkfifo " + this._GetGeneratedFileName(job) + "\n" +
         "./" + dbgen_path + " " + dbgen_args.join(" ") + " &\n" +
         "zstd " + this._GetGeneratedFileName(job) + " --stdout | aws s3 cp - " + this.s3_wrapper._GetS3AccessPath(job.DATABASE_ID, job.TABLE_NAME, job.STEP) + "\n";

      // Write script to file
      if (!fs.existsSync(csv_directory)) {
         fs.mkdirSync(csv_directory);
      }
      const script_file_name = csv_directory + "/script.sh";
      fs.writeFileSync(script_file_name, script_file_content);
      await this._RunCommand("chmod", ["u+x", script_file_name]);

      // Run script
      await this._RunCommand("./" + script_file_name, []);
      this.stats.pipelined_upload_total += (Date.now() - start);
      this.stats.pipelined_upload.push(Date.now() - start);
   }

   async CopyIntoTableSnowflake(job) {
      const start = Date.now();
      const options = "FILE_FORMAT = (type = csv field_delimiter = '|' skip_header = 0 ERROR_ON_COLUMN_COUNT_MISMATCH = false) FORCE = TRUE";
      const s3_options = "CREDENTIALS = (AWS_KEY_ID = '" + this.s3_wrapper.connection_options.accessKeyId + "' AWS_SECRET_KEY = '" + this.s3_wrapper.connection_options.secretAccessKey + "')";
      const table_db_id = job.TABLE_NAME + "_" + job.DATABASE_ID;
      const file_name = this.s3_wrapper._GetS3AccessPath(job.DATABASE_ID, job.TABLE_NAME, job.STEP);
      const command = "copy into " + table_db_id + " from " + file_name + " " + s3_options + " " + options + ";";
      console.log(chalk.cyan("\nCopy into snowflake table ..."));
      console.log(command);
      const res = await this.snowflake.RunSync(command);
      console.log(res.rows);
      if (res.rows[0].errors_seen !== 0) {
         console.log(chalk.red("error while copying rows"));
         process.exit();
      }
      this.stats.copy_snowflake_total += (Date.now() - start);
      this.stats.copy_snowflake.push({time: Date.now() - start, table_name: job.TABLE_NAME});
   }

   async MarkJobAsDone(job) {
      console.log(chalk.cyan("\nMarking job as complete ..."));
      const res = await this.snowflake.RunSync("update jobs set status = 'complete' where job_id = ? and status = 'running'", [job.JOB_ID]);
      if (res.rows.length !== 1 || res.rows[0]['number of rows updated'] !== 1) {
         throw new Error("Unexpected result on job completion: " + JSON.stringify(res));
      }
      console.log("done");
   }

   async CleanTmpFiles(job) {
      fs.rmSync(csv_directory, {recursive: true, force: true});
   }
}

async function main() {
   const worker = new LoadWorker();

   let job;
   while (job = await worker.GetNextJob()) {
      await worker.CreateCsvFile(job);
      await worker.CompressCsvFile(job);
      await worker.UploadCsvFileS3(job);
      await worker.CopyIntoTableSnowflake(job);
      await worker.MarkJobAsDone(job);
      await worker.CleanTmpFiles(job);
   }

   console.log(chalk.cyan("\nNormal program exit: done :)"));
   console.log(JSON.stringify(worker.stats));
}

main();
