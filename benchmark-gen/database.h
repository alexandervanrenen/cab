#pragma once

#include "common.h"

#include <iostream>
#include <vector>
#include <string>
#include <cmath>
#include <cstdlib>

struct Database {
   uint64_t database_id;

   uint64_t scale_factor; // TPC-H scale factor
   uint64_t GetByteCount() const { return scale_factor * 1_GB; }
   uint64_t GetSizeBucket() const { return round(log(scale_factor * 1_GB) / log(10)); }

   bool is_read_only = false;
   uint64_t cpu_time; // In a full day

   std::vector<double> query_count_slots;
   uint32_t pattern_id;
   std::string pattern_description;

   struct Query {
      uint32_t start;
      uint32_t query_id;
      std::vector<std::string> arguments;
   };
   std::vector<Query> queries;

   void Write(std::ostream &os) const
   {
      const char nl = '\n';
      os << "database_id " << database_id << nl;
      os << "scale_factor " << scale_factor << nl;
      os << "database_byte_count " << GetByteCount() << nl;
      os << "size_bucket " << GetSizeBucket() << nl;
      os << "cpu_time " << cpu_time << nl;
      os << "cpu_time_h " << (cpu_time / 1e6 / 3600) << "h" << nl;
      os << "queries " << queries.size() << nl;
      for (auto &query: queries) {
         os << query.query_id << " " << query.start << " ";
         for (uint32_t idx = 0; idx<query.arguments.size(); idx++) {
            os << (idx>0 ? "," : "") << query.arguments[idx];
         }
         os << nl;
      }
   }

   void WriteJson(std::ostream &os) const
   {
      const char nl = '\n';
      os << "{" << nl;
      os << "  \"database_id\": " << database_id << "," << nl;
      os << "  \"scale_factor\": " << scale_factor << "," << nl;
      os << "  \"database_byte_count\": " << GetByteCount() << "," << nl;
      os << "  \"size_bucket\": " << GetSizeBucket() << "," << nl;
      os << "  \"pattern_id\": " << pattern_id << "," << nl;
      os << "  \"cpu_time\": " << cpu_time << "," << nl;
      os << "  \"cpu_time_h\":" << "\"" << (cpu_time / 1e6 / 3600) << "h\"" << "," << nl;
      os << "  \"query_count\": " << queries.size() << "," << nl;
      os << "  \"queries\": [" << nl << "    ";
      for (uint32_t idx = 0; idx<queries.size(); idx++) {
         auto &query = queries[idx];
         os << "{" << nl;
         os << "      \"query_id\": " << query.query_id << "," << nl;
         os << "      \"start\": " << query.start << "," << nl;
         os << "      \"arguments\": [";
         for (uint32_t idx = 0; idx<query.arguments.size(); idx++) {
            os << (idx>0 ? "," : "") << query.arguments[idx];
         }
         os << "]" << nl;
         os << "    }" << (idx + 1<queries.size() ? "," : "");
      }
      os << nl << "  ]" << nl;
      os << "}" << nl;
   }

   void Read(std::istream &is)
   {
      ReadMetaOnly(is);
      std::string buffer;
      uint64_t query_count;
      is >> buffer >> query_count;

      for (uint64_t query_idx = 0; query_idx<query_count; query_idx++) {
         Query query;
         is >> query.query_id >> query.start;
         is.get(); // skip the space character
         std::getline(is, buffer);
         query.arguments = Split(buffer, ',');
         queries.push_back(query);
      }
   }

   void ReadMetaOnly(std::istream &is)
   {
      std::string buffer;
      is >> buffer >> database_id;
      is >> buffer >> scale_factor;
      is >> buffer >> buffer; // database_byte_count
      is >> buffer >> buffer; // size_bucket
      is >> buffer >> cpu_time;
      is >> buffer >> buffer; // cpu_time_h
   }

private:
   static std::vector<std::string> Split(const std::string &str, char splitter)
   {
      std::vector<std::string> result;
      std::string a = str;
      for (size_t i = a.find_first_of(splitter); i != std::string::npos; i = a.find_first_of(splitter)) {
         if (a.find_first_of(splitter) == std::string::npos)
            throw;
         result.push_back(a.substr(0, i));
         a = a.substr(i + 1, a.size());
      }
      if (!a.empty())
         result.push_back(a.substr(0, a.size()));
      return result;
   }
};