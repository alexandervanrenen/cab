#include "database.h"

#include <iostream>
#include <vector>
#include <array>
#include <random>
#include <map>
#include <algorithm>
#include <fstream>
#include <sstream>
#include <unordered_set>
#include <cassert>
#include <cmath>

using namespace std;

struct Utility {
   static vector<double> GenerateDeterministicLogNormal(uint64_t count, double mean, double sd, uint64_t cut_off, uint64_t seed)
   {
      // Generate large sample
      mt19937 gen(seed);
      lognormal_distribution<> dist(mean, sd);
      vector<double> over_samples(1000 * count);
      for (double &over_sample : over_samples) {
         over_sample = dist(gen);
      }
      sort(over_samples.begin(), over_samples.end());

      // Sample the sample
      vector<double> result(count);
      double segment_count = count + cut_off * 2 + 1;
      double segment_count_width = over_samples.size() / segment_count;
      for (uint64_t i = cut_off; i<count + cut_off * 2; i++) {
         result[i] = over_samples[segment_count_width * i]; // TODO: fix don't start at 0
      }

      return result;
   }

   // https://en.wikipedia.org/wiki/Triangular_distribution
   static double GenerateTriangleValue(double a, double b, double c, mt19937 &gen)
   {
      uniform_real_distribution dist;
      double U = dist(gen);
      double F = (c - a) / (b - a);
      if (U<F) {
         return a + sqrt(U * (b - a) * (c - a));
      } else {
         return b - sqrt((1 - U) * (b - a) * (b - c));
      }
   }

   static double GenerateNonExtremeNormal(double mean, double sd, mt19937 &gen)
   {
      normal_distribution<> dist(mean, sd);
      double result = dist(gen);
      while (abs(result - mean)>2.0 * sd) { // ~95% of values are fine
         result = dist(gen);
      }
      return result;
   }
};

struct TpchQueries {
   struct Arguments {
      // TPC-H 4.2.3
      inline constexpr static std::array<const char *, 5> regions = {"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"};
      inline constexpr static std::array<int32_t, 25> nation_to_region = {0, 1, 1, 1, 4, 0, 3, 3, 2, 2, 4, 4, 2, 4, 0, 0, 0, 1, 2, 3, 4, 2, 3, 3, 1};
      inline constexpr static std::array<const char *, 25> nations = {"ALGERIA", "ARGENTINA", "BRAZIL", "CANADA", "EGYPT", "ETHIOPIA", "FRANCE", "GERMANY", "INDIA", "INDONESIA", "IRAN", "IRAQ", "JAPAN", "JORDAN", "KENYA", "MOROCCO", "MOZAMBIQUE", "PERU", "CHINA", "ROMANIA", "SAUDI ARABIA", "VIETNAM", "RUSSIA", "UNITED KINGDOM", "UNITED STATES"};

      // TPC-H 4.2.2.13
      struct Type {
         inline constexpr static std::array<const char *, 6> syllable1 = {"STANDARD", "SMALL", "MEDIUM", "LARGE", "ECONOMY", "PROMO"};
         inline constexpr static std::array<const char *, 5> syllable2 = {"ANODIZED", "BURNISHED", "PLATED", "POLISHED", "BRUSHED"};
         inline constexpr static std::array<const char *, 5> syllable3 = {"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"};
      };
      struct Container {
         inline constexpr static std::array<const char *, 5> syllable1 = {"SM", "LG", "MED", "JUMBO", "WRAP"};
         inline constexpr static std::array<const char *, 8> syllable2 = {"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"};
      };
      inline constexpr static std::array<const char *, 5> segments = {"AUTOMOBILE", "BUILDING", "FURNITURE", "MACHINERY", "HOUSEHOLD"};
      inline constexpr static std::array<const char *, 92> colors = {"almond", "antique", "aquamarine", "azure", "beige", "bisque", "black", "blanched", "blue", "blush", "brown", "burlywood", "burnished", "chartreuse", "chiffon", "chocolate", "coral", "cornflower", "cornsilk", "cream", "cyan", "dark", "deep", "dim", "dodger", "drab", "firebrick", "floral", "forest", "frosted", "gainsboro", "ghost", "goldenrod", "green", "grey", "honeydew", "hot", "indian", "ivory", "khaki", "lace", "lavender", "lawn", "lemon", "light", "lime", "linen", "magenta", "maroon", "medium", "metallic", "midnight", "mint", "misty", "moccasin", "navajo", "navy", "olive", "orange", "orchid", "pale", "papaya", "peach", "peru", "pink", "plum", "powder", "puff", "purple", "red", "rose", "rosy", "royal", "saddle", "salmon", "sandy", "seashell", "sienna", "sky", "slate", "smoke", "snow", "spring", "steel", "tan", "thistle", "tomato", "turquoise", "violet", "wheat", "white", "yellow"};
      inline constexpr static std::array<const char *, 7> mode = {"REG AIR", "AIR", "RAIL", "SHIP", "TRUCK", "MAIL", "FOB"};
   };

   struct UpdateState {
      uint32_t update_count; // for one rotation; splits relation into 'update_count' blocks that are updated together
      uint32_t rotation; // which of the keys 0: 0-7, 1: 8-15, 2: 16-23, 3: 24-31
      uint32_t update; // current update update_count ... 1

      explicit UpdateState(uint32_t update_count)
              : update_count(update_count)
                , rotation(0)
                , update(update_count) {}

      std::vector<std::string> GenerateNext(uint64_t scale_factor)
      {
         const uint64_t order_count = scale_factor * 1500000;
         const uint64_t max_order_key = 1 + order_count * 4;
         const uint64_t group_count = 1 + max_order_key / 32;
         const uint64_t first_group = (group_count * (update - 1)) / update_count;
         const uint64_t last_group = (group_count * update) / update_count;
         assert(group_count>=update_count);

         std::vector<std::string> result;
         result.push_back(to_string(first_group * 32));
         result.push_back(to_string(last_group * 32));
         result.push_back(to_string(rotation * 8));
         result.push_back(to_string((rotation + 1) * 8 - 1));

         update--;
         if (update == 0) {
            update = update_count;
            rotation = (rotation + 1) % 4;
         }

         return result;
      }
   };

   static string TwoDigitNumber(uint32_t num)
   {
      return (num<10 ? "0" : "") + to_string(num);
   }

   static string QStr(const string &str)
   {
      return "\"" + str + "\"";
   }

   static vector<string> GenerateQueryArguments(int query_id, uint64_t scale_factor, mt19937 &gen, UpdateState &update_state)
   {
      string q = "\"";
      switch (query_id) {
         case 1: {
            uniform_int_distribution<int32_t> dist1(60, 120);
            return vector<string>{to_string(dist1(gen))};
         }
         case 2: {
            uniform_int_distribution<int32_t> dist1(1, 50);
            uniform_int_distribution<int32_t> dist2(0, Arguments::Type::syllable3.size() - 1);
            uniform_int_distribution<int32_t> dist3(0, Arguments::regions.size() - 1);
            return vector<string>{to_string(dist1(gen)), QStr(Arguments::Type::syllable3[dist2(gen)]), QStr(Arguments::regions[dist3(gen)])};
         }
         case 3: {
            uniform_int_distribution<int32_t> segment(0, Arguments::segments.size() - 1);
            uniform_int_distribution<int32_t> day(1, 31);
            return vector<string>{QStr(Arguments::segments[segment(gen)]), QStr("1995-03-" + TwoDigitNumber(day(gen)))};
         }
         case 4: {
            uniform_int_distribution<int32_t> dist1(0, 57);
            uint32_t month_offset = dist1(gen);
            uint32_t year = 3 + month_offset / 12;
            uint32_t month = 1 + month_offset % 12;
            return vector<string>{QStr("199" + to_string(year) + "-" + TwoDigitNumber(month) + "-01")};
         }
         case 5: {
            uniform_int_distribution<int32_t> region(0, Arguments::regions.size() - 1);
            uniform_int_distribution<int32_t> year(1993, 1997);
            return vector<string>{QStr(Arguments::regions[region(gen)]), QStr(to_string(year(gen)) + "-01-01")};
         }
         case 6: {
            uniform_int_distribution<int32_t> date(1993, 1997);
            uniform_int_distribution<int32_t> discount(2, 9);
            uniform_int_distribution<int32_t> quantity(24, 25);
            return vector<string>{QStr(to_string(date(gen)) + "-01-01"), to_string(discount(gen)), to_string(quantity(gen))}; // note: [discount] / 100 in the query
         }
         case 7: {
            uniform_int_distribution<int32_t> nation(0, Arguments::nations.size() - 1);
            return vector<string>{QStr(Arguments::nations[nation(gen)]), QStr(Arguments::nations[nation(gen)])};
         }
         case 8: {
            uniform_int_distribution<int32_t> nation(0, Arguments::nations.size() - 1);
            int32_t selected_nation_idx = nation(gen);
            uniform_int_distribution<int32_t> type1(0, Arguments::Type::syllable1.size() - 1);
            uniform_int_distribution<int32_t> type2(0, Arguments::Type::syllable2.size() - 1);
            uniform_int_distribution<int32_t> type3(0, Arguments::Type::syllable3.size() - 1);
            return vector<string>{QStr(Arguments::nations[selected_nation_idx]), QStr(Arguments::regions[Arguments::nation_to_region[selected_nation_idx]]), //nl
                    QStr(string(Arguments::Type::syllable1[type1(gen)]) + " " + Arguments::Type::syllable2[type2(gen)] + " " + Arguments::Type::syllable3[type3(gen)])};
         }
         case 9: {
            uniform_int_distribution<int32_t> color(0, Arguments::colors.size() - 1);
            return vector<string>{QStr(Arguments::colors[color(gen)])};
         }
         case 10: {
            uniform_int_distribution<int32_t> dist1(1, 24);
            uint32_t month_offset = dist1(gen);
            uint32_t year = 3 + month_offset / 12;
            uint32_t month = 1 + month_offset % 12;
            return vector<string>{QStr("199" + to_string(year) + "-" + TwoDigitNumber(month) + "-01")};
         }
         case 11: {
            uniform_int_distribution<int32_t> nation(0, Arguments::nations.size() - 1);
            return vector<string>{QStr(Arguments::nations[nation(gen)]), to_string(scale_factor)}; // note: 0.0001 / [scale_factor] in the query
         }
         case 12: {
            uint32_t mode_1 = 0;
            uint32_t mode_2 = 0;
            uniform_int_distribution<int32_t> mode(0, Arguments::mode.size() - 1);
            while (mode_1 == mode_2) {
               mode_1 = mode(gen);
               mode_2 = mode(gen);
            }
            uniform_int_distribution<int32_t> year(1993, 1997);
            return vector<string>{QStr(Arguments::mode[mode_1]), QStr(Arguments::mode[mode_2]), QStr(to_string(year(gen)) + "-01-01")};
         }
         case 13: {
            constexpr static std::array<const char *, 4> word_1 = {"special", "pending", "unusual", "express"};
            constexpr static std::array<const char *, 4> word_2 = {"packages", "requests", "accounts", "deposits"};
            uniform_int_distribution<int32_t> word(0, 3);
            return vector<string>{QStr(word_1[word(gen)]), QStr(word_2[word(gen)])};
         }
         case 14: {
            uniform_int_distribution<int32_t> dist1(0, 59);
            uint32_t month_offset = dist1(gen);
            uint32_t year = 3 + month_offset / 12;
            uint32_t month = 1 + month_offset % 12;
            return vector<string>{QStr("199" + to_string(year) + "-" + TwoDigitNumber(month) + "-01")};
         }
         case 15: {
            uniform_int_distribution<int32_t> dist1(0, 57);
            uint32_t month_offset = dist1(gen);
            uint32_t year = 3 + month_offset / 12;
            uint32_t month = 1 + month_offset % 12;
            return vector<string>{QStr("199" + to_string(year) + "-" + TwoDigitNumber(month) + "-01")};
         }
         case 16: {
            vector<string> result;
            uniform_int_distribution<int32_t> brand(1, 5);
            result.push_back(QStr("Brand#" + to_string(brand(gen)) + to_string(brand(gen))));
            uniform_int_distribution<int32_t> type1(0, Arguments::Type::syllable1.size() - 1);
            uniform_int_distribution<int32_t> type2(0, Arguments::Type::syllable2.size() - 1);
            result.push_back(QStr(Arguments::Type::syllable1[type1(gen)] + string(" ") + Arguments::Type::syllable2[type2(gen)]));
            unordered_set<int32_t> generated_sizes;
            uniform_int_distribution<int32_t> size_dist(1, 50);
            while (generated_sizes.size() != 8) {
               int32_t size = size_dist(gen);
               if (generated_sizes.count(size) == 0) {
                  generated_sizes.insert(size);
                  result.push_back(to_string(size));
               }
            }
            return result;
         }
         case 17: {
            uniform_int_distribution<int32_t> brand(1, 5);
            uniform_int_distribution<int32_t> container1(0, Arguments::Container::syllable1.size() - 1);
            uniform_int_distribution<int32_t> container2(0, Arguments::Container::syllable2.size() - 1);
            return vector<string>{QStr("Brand#" + to_string(brand(gen)) + to_string(brand(gen))), //nl
                    QStr(Arguments::Container::syllable1[container1(gen)] + string(" ") + Arguments::Container::syllable2[container2(gen)])};
         }
         case 18: {
            uniform_int_distribution<int32_t> quantity(312, 315);
            return vector<string>{to_string(quantity(gen))};
         }
         case 19: {
            vector<string> result;
            uniform_int_distribution<int32_t> brand(1, 5);
            result.push_back(QStr("Brand#" + to_string(brand(gen)) + to_string(brand(gen))));
            result.push_back(QStr("Brand#" + to_string(brand(gen)) + to_string(brand(gen))));
            result.push_back(QStr("Brand#" + to_string(brand(gen)) + to_string(brand(gen))));
            uniform_int_distribution<int32_t> quantity1(1, 10);
            uniform_int_distribution<int32_t> quantity2(10, 20);
            uniform_int_distribution<int32_t> quantity3(20, 30);
            result.push_back(to_string(quantity1(gen)));
            result.push_back(to_string(quantity2(gen)));
            result.push_back(to_string(quantity3(gen)));
            return result;
         }
         case 20: {
            uniform_int_distribution<int32_t> color(0, Arguments::colors.size() - 1);
            uniform_int_distribution<int32_t> year(1993, 1997);
            uniform_int_distribution<int32_t> nation(0, Arguments::nations.size() - 1);
            return vector<string>{QStr(Arguments::colors[color(gen)]), QStr(to_string(year(gen)) + "-01-01"), QStr(Arguments::nations[nation(gen)])};
         }
         case 21: {
            uniform_int_distribution<int32_t> nation(0, Arguments::nations.size() - 1);
            return vector<string>{QStr(Arguments::nations[nation(gen)])};
         }
         case 22: {
            vector<string> result;
            unordered_set<int32_t> generated_country_codes;
            uniform_int_distribution<int32_t> nation_dist(0, Arguments::nations.size() - 1);
            while (generated_country_codes.size() != 7) {
               int32_t country_code = nation_dist(gen) + 10;
               if (generated_country_codes.count(country_code) == 0) {
                  generated_country_codes.insert(country_code);
                  result.push_back(to_string(country_code));
               }
            }
            return result;
         }
         case 23: {
            return update_state.GenerateNext(scale_factor);
         }
         default: {
            throw;
         }
      }
   }
};

struct Vector {
   static void AddSinusHead(vector<double> &scales, double intensity, double start_ratio, double width)
   {
      double a = 3.1415 / (scales.size() * width);
      double b = 3.1415 * ((start_ratio + (0.5 * (1 - width))) / width);

      for (uint32_t i = 0; i<scales.size(); i++) {
         double x = i * a - b;
         if (0<x && x<3.1415) { // Draw only one hump
            double val = sin(x) * intensity;
            if (val>0) {
               scales[i] += val;
            }
         }
      }
   }

   static void AddRandomNoise(vector<double> &scales, mt19937 &gen, double intensity, double likelihood)
   {
      uniform_real_distribution dist(0.0, 1.0);
      for (double &scale : scales) {
         if (dist(gen)<=likelihood) {
            auto fac = dist(gen);
            scale += intensity * fac;
            if (scale<0) {
               scale = 0;
            }
         }
      }
   }

   static void AddSequence(vector<double> &scales, double intensity, double start_ratio, double length_ratio)
   {
      uint32_t start = start_ratio * scales.size();
      uint32_t length = length_ratio * scales.size();
      for (uint32_t pos = start; pos<start + length; pos++) {
         double &scale = scales[pos % scales.size()];
         scale += intensity;
         if (scale<0) {
            scale = 0;
         }
      }
   }

   static void OnOffPattern(vector<double> &scales, mt19937 &gen, double intensity, uint32_t spike_count, double length)
   {
      double spike_width = double(scales.size()) / spike_count;
      for (uint32_t spike_idx = 0; spike_idx<spike_count; spike_idx++) {
         uint32_t spike_start = spike_idx * spike_width;
         uint32_t spike_end = spike_start + spike_width * length;
         for (uint32_t pos = spike_start; pos<spike_end; pos++) {
            double &scale = scales[pos % scales.size()];
            scale += intensity;
            if (scale<0) {
               scale = 0;
            }
         }
      }
   }

   static void OnOffPatternNoise(vector<double> &scales, mt19937 &gen, double intensity, uint32_t spike_count, double length)
   {
      uniform_real_distribution dist(0.0, 1.0);
      double spike_width = double(scales.size()) / spike_count;
      for (uint32_t spike_idx = 0; spike_idx<spike_count; spike_idx++) {
         uint32_t spike_start = spike_idx * spike_width;
         uint32_t spike_end = spike_start + spike_width * length;
         double spike_height = dist(gen);
         for (uint32_t pos = spike_start; pos<spike_end; pos++) {
            double &scale = scales[pos % scales.size()];
            scale += intensity * spike_height;
            if (scale<0) {
               scale = 0;
            }
         }
      }
   }

   static void AddSequenceRandomNoise(vector<double> &scales, mt19937 &gen, double intensity, double start_ratio, double length_ratio)
   {
      uniform_real_distribution dist(0.0, 1.0);
      uint32_t start = start_ratio * scales.size();
      uint32_t length = length_ratio * scales.size();
      for (uint32_t pos = start; pos<start + length; pos++) {
         double &scale = scales[pos % scales.size()];
         scale += intensity * dist(gen);
         if (scale<0) {
            scale = 0;
         }
      }
   }

   static void AddRandomWalk(vector<double> &scales, mt19937 &gen, double intensity, double start_ratio, double length_ratio)
   {
      uniform_real_distribution dist(0.0, 1.0);
      uint32_t start = start_ratio * scales.size();
      uint32_t length = length_ratio * scales.size();
      double diff = 0.1 * intensity;
      double state = dist(gen) * intensity;
      for (uint32_t pos = start; pos<start + length; pos++) {
         if (dist(gen)>=0.5) {
            state += diff;
         } else {
            state -= diff;
         }
         double &scale = scales[pos % scales.size()];
         scale += state;
         if (scale<0) {
            scale = 0;
         }
         if (scale>intensity) {
            scale = intensity;
         }
      }
   }

   static vector<uint64_t> ToCpuTime(vector<double> &scales, uint64_t total_cpu_time)
   {
      double sum = Sum(scales);
      assert(sum>0);

      vector<uint64_t> cpu_time_in_slot(scales.size());
      for (uint32_t idx = 0; idx<scales.size(); idx++) {
         cpu_time_in_slot[idx] = total_cpu_time * scales[idx] / sum;
      }
      return cpu_time_in_slot;
   }

   static double Sum(vector<double> &scales)
   {
      double sum = 0.0;
      for (double scale : scales) {
         assert(scale>=0);
         sum += scale;
      }
      return sum;
   }
};

struct Generator {
   static uint64_t GetSeedForDatabaseCount() { return 6; }
   static uint64_t GetSeedForQueryCount() { return 28; }
   static uint64_t GetSeedForPatterns() { return 496; }
   static uint64_t GetSeedForQueries() { return 8128; }
   static uint64_t GetSeedForQueryArguments() { return 33550336; }

   vector<Database> databases;

   static double SizeToScaleFactor(double size)
   {
      const double scale_factor = std::round(size / 1_GB);
      return scale_factor == 0 ? 1 : scale_factor;
   }

   void GenerateDatabases(uint64_t database_count, uint64_t total_size)
   {
      vector<double> db_sizes;

      // Q200
      double mean = 24.66794;
      double sd = 2.575434;
      while (true) {
         db_sizes = Utility::GenerateDeterministicLogNormal(database_count, mean, sd, 0, GetSeedForDatabaseCount());
         double sum = 0;
         for (auto iter : db_sizes) {
            sum += SizeToScaleFactor(iter) * 1e9;
         }

         if (sum>total_size) {
            mean *= 0.95;
            sd *= 0.95;
            continue;
         }
         if (sum<0.95 * total_size) {
            mean *= 1.05;
            sd *= 1.05;
            continue;
         }

         break;
      }

      this->databases.resize(db_sizes.size());
      for (uint64_t idx = 0; idx<db_sizes.size(); idx++) {
         this->databases[idx].database_id = idx;
         this->databases[idx].scale_factor = SizeToScaleFactor(db_sizes[idx]);
      }
   }

   void GenerateFixedDatabases(uint64_t database_count, uint64_t scale_factor, uint64_t cpu_hours)
   {
      this->databases.resize(database_count);
      for (uint64_t idx = 0; idx<database_count; idx++) {
         this->databases[idx].database_id = idx;
         this->databases[idx].scale_factor = scale_factor;
         this->databases[idx].cpu_time = cpu_hours * 3600e6;
      }
   }

   // A database of a particular size has a certain query count .. it's logtriangle distributed -> these are the values for the triangle distribution
   static uint64_t RollQueryCount(const Database &database, mt19937 &gen)
   {
      uint32_t size_bucket = database.GetSizeBucket();
      switch (size_bucket) {
         case 8:
         case 9: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.7, 3, gen));
         case 10: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.7, 4.1, gen));
         case 11: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.6, 3.2, gen));
         case 12: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.7, 4.8, gen));
         case 13: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.7, 5.9, gen));
         case 14: return pow(2.718282, Utility::GenerateTriangleValue(0, 11.7, 5.5, gen));
         default: throw;
      }
   }

   // A database of a particular size uses a certain cpu time .. it's lognormal distributed -> these are the values for the normal distribution
   static uint64_t RollCpuTime(const Database &database, mt19937 &gen)
   {
      uint32_t size_bucket = database.GetSizeBucket();
      switch (size_bucket) {
         case 8:
         case 9: return pow(2.718282, Utility::GenerateNonExtremeNormal(19.81281, 2.847744, gen));
         case 10: return pow(2.718282, Utility::GenerateNonExtremeNormal(21.51081, 2.949972, gen));
         case 11: return pow(2.718282, Utility::GenerateNonExtremeNormal(22.30084, 3.469075, gen));
         case 12: return pow(2.718282, Utility::GenerateNonExtremeNormal(23.72666, 3.349028, gen));
         case 13: return pow(2.718282, Utility::GenerateNonExtremeNormal(24.03537, 3.401711, gen)); // Adjusted: 25 -> 24 ;)
         case 14: return pow(2.718282, Utility::GenerateNonExtremeNormal(24.32934, 4.289488, gen));
         default: throw;
      }
   }

   void GenerateCpuTimeForDatabases(uint64_t total_cpu_hours)
   {
      mt19937 gen(GetSeedForQueryCount());
      uint64_t actual_cpu_time = 0;
      for (auto &database : databases) {
         // database.query_count = RollQueryCount(database, gen); // Not needed because we just use the cpu time to generate queries
         database.cpu_time = RollCpuTime(database, gen);
         actual_cpu_time += database.cpu_time;
      }

      uint64_t wanted_cpu_time = total_cpu_hours * 3600 * 1e6;
      for (auto &database : databases) {
         database.cpu_time = database.cpu_time * (wanted_cpu_time * 1.0 / actual_cpu_time);
      }
   }

   // Each query has 100 time slots. This method assigns a query count to each time slot.
   void GenerateQueryArrivalDistribution()
   {
      mt19937 gen(GetSeedForPatterns());
      uniform_real_distribution<double> rdist(0.0, 1.0);

      // Define different query arrival patterns throughout the time
      struct Pattern {
         uint32_t pattern_id;
         string description;
         double likelihood;
         function<void(vector<double> &scales)> generate;
      };
      vector<Pattern> patterns;
      patterns.push_back({1, "Random with a few sines", 10.0, [&](vector<double> &scales) {
         Vector::AddSequence(scales, 0.2, 0, 1.0);
         Vector::AddSequenceRandomNoise(scales, gen, 1.0, 0, 1.0);
         for (uint32_t i = 0; i<8; i++) {
            Vector::AddSinusHead(scales, 0.5, 0.6 * rdist(gen) - 0.3, 0.05 + rdist(gen) * 0.05);
         }
         Vector::AddRandomNoise(scales, gen, 1.0, 0.1 * rdist(gen));
      }});
      patterns.push_back({3, "A few random spikes", 17.0, [&](vector<double> &scales) {
         exponential_distribution<double> exp_dist(0.5);
         uint32_t iteration_count = round(exp_dist(gen) * 2 + 1);
         for (uint32_t i = 0; i<iteration_count; i++) {
            Vector::AddRandomWalk(scales, gen, 1.0, 0.1 + rdist(gen) * 0.8, 0.1 * rdist(gen));
         }
      }});
      patterns.push_back({4, "One short burst column", 21.0, [&](vector<double> &scales) {
         Vector::AddRandomWalk(scales, gen, 1.0, 0.1 + rdist(gen) * 0.8, 0.15 + 0.1 * rdist(gen));
      }});
      patterns.push_back({5, "Constant load with a sudden break", 12.0, [&](vector<double> &scales) {
         Vector::AddSequence(scales, 4.0, 0.0, 1.0);
         if (rdist(gen)<0.5) {
            Vector::AddSequence(scales, 6.0, rdist(gen), 0.05 + 0.15 * rdist(gen));
         }
         if (rdist(gen)<0.5) {
            Vector::AddSequence(scales, 6.0, rdist(gen), 0.05 + 0.15 * rdist(gen));
         }
         if (rdist(gen)<0.5) {
            Vector::AddSequence(scales, 6.0, rdist(gen), 0.05 + 0.15 * rdist(gen));
         }
         if (rdist(gen)<0.5) {
            Vector::AddSequence(scales, -100, rdist(gen), 0.05 + 0.15 * rdist(gen));
         }
      }});
      patterns.push_back({6, "Spikes every hour", 20.0, [&](vector<double> &scales) {
         uint32_t level = (rdist(gen) * 2);
         Vector::AddSequence(scales, 2 * level, 0.0, 1.0);
         if (rdist(gen)<0.5 && level>0) {
            Vector::OnOffPatternNoise(scales, gen, 2.0 + 5.0 * rdist(gen), 24, 0.4 + rdist(gen) * 0.2);
         } else {
            Vector::OnOffPattern(scales, gen, 1.0 + 5.0 * rdist(gen), 24, 0.4 + rdist(gen) * 0.2);
         }
      }});

      // Setup random number generator to pick a pattern, given their likelihoods
      double total_likelihood = accumulate(patterns.begin(), patterns.end(), 0.0, [](double sum, const Pattern &p) { return sum + p.likelihood; });
      assert(total_likelihood>0.0);
      uniform_real_distribution pattern_dist(0.0, total_likelihood);

      // Assign a pattern to each database
      for (auto &database: databases) {
         start:
         const bool use_ids = false;
         if (!use_ids) {
            // Choose a pattern
            double pattern_pick = pattern_dist(gen);
            for (auto &iter : patterns) {
               if (pattern_pick>iter.likelihood) {
                  pattern_pick -= iter.likelihood;
                  continue;
               }

               database.query_count_slots.clear();
               database.query_count_slots.resize(100, 0);
               iter.generate(database.query_count_slots);

               database.pattern_id = iter.pattern_id;
               database.pattern_description = iter.description;
               break;
            }
         } else {
            auto &pattern = patterns[database.database_id];

            database.query_count_slots.clear();
            database.query_count_slots.resize(100, 0);
            pattern.generate(database.query_count_slots);

            database.pattern_id = pattern.pattern_id;
            database.pattern_description = pattern.description;
         }

         // Just to be save, in case randomness goes mad :D
         if (Vector::Sum(database.query_count_slots) == 0) {
            goto start;
         }
      }
   }

   static uint64_t EstimateTimeForQuery(uint32_t query_id, uint64_t scale_factor)
   {
      switch (query_id) { // Times 8, because of 8 cores per machine
         case 1: return scale_factor * 149560 * 8;
         case 2: return scale_factor * 55150 * 8;
         case 3: return scale_factor * 112050 * 8;
         case 4: return scale_factor * 58190 * 8;
         case 5: return scale_factor * 66190 * 8;
         case 6: return scale_factor * 17490 * 8;
         case 7: return scale_factor * 61090 * 8;
         case 8: return scale_factor * 67480 * 8;
         case 9: return scale_factor * 127850 * 8;
         case 10: return scale_factor * 95110 * 8;
         case 11: return scale_factor * 15720 * 8;
         case 12: return scale_factor * 64200 * 8;
         case 13: return scale_factor * 146020 * 8;
         case 14: return scale_factor * 29000 * 8;
         case 15: return scale_factor * 46390 * 8;
         case 16: return scale_factor * 32790 * 8;
         case 17: return scale_factor * 42430 * 8;
         case 18: return scale_factor * 205680 * 8;
         case 19: return scale_factor * 51580 * 8;
         case 20: return scale_factor * 46310 * 8;
         case 21: return scale_factor * 113140 * 8;
         case 22: return scale_factor * 19820 * 8;
         case 23: return scale_factor * (72530 + 97780) * 8;
         default: throw;
      }
   }

   static uint64_t AverageQueryTime(uint64_t scale_factor)
   {
      uint64_t sum = 0;
      for (uint32_t query_id = 1; query_id<=23; query_id++) {
         sum += EstimateTimeForQuery(query_id, scale_factor);
      }
      return sum / 23;
   }

   static uint32_t GetRandomQuery(mt19937 &gen, Database &database)
   {
      if (database.is_read_only) {
         uniform_int_distribution<uint32_t> query_dist(1, 22);
         return query_dist(gen);
      } else {
         uniform_int_distribution<uint32_t> query_dist(1, 23);
         uint32_t query_id = query_dist(gen);
//         while (query_id == 5 || query_id == 7 || query_id == 13 || query_id == 21) {
//            query_id = query_dist(gen);
//         }
         return query_id;
      }
   }

   // Generates queries for each time slot using the exponential distribution.
   void GenerateQueryArrivalTimes(uint64_t total_duration_in_hours)
   {
      mt19937 gen(GetSeedForQueries());

      for (auto &database: databases) {
         // Distribute the cpu time over slots
         uint64_t total_cpu_time = database.cpu_time;
         vector<uint64_t> cpu_time_slots = Vector::ToCpuTime(database.query_count_slots, total_cpu_time);

         // Timing
         uint32_t now_ms = 0; // in ms
         uint32_t ms_per_slot = (total_duration_in_hours * 3600 * 1000) / cpu_time_slots.size();
         uint64_t average_query_cup_time = AverageQueryTime(database.scale_factor);

         // Assign queries to each slot using exponential arrival pattern
         for (uint64_t slot_idx = 0; slot_idx<cpu_time_slots.size(); slot_idx++) {
            uint64_t cpu_time_in_slot = cpu_time_slots[slot_idx];
            if (cpu_time_in_slot == 0) {
               continue;
            }

            uint64_t query_count_in_slot = cpu_time_in_slot / average_query_cup_time;
            if (query_count_in_slot<1) {
               query_count_in_slot = 1;
            }
            double rate_per_second = query_count_in_slot / (ms_per_slot / 1000.0);
            exponential_distribution dist_s(rate_per_second);
            uint32_t slot_start = ms_per_slot * slot_idx;
            now_ms = max(now_ms, slot_start);

            uint64_t generated_cpu_time = 0;
            while (generated_cpu_time<cpu_time_in_slot) {
               double distance_s = dist_s(gen);
               now_ms += distance_s * 1000.0;
               now_ms = (now_ms - slot_start) % ms_per_slot + slot_start; // loop in this slot
               uint32_t query_id = GetRandomQuery(gen, database);
               generated_cpu_time += EstimateTimeForQuery(query_id, database.scale_factor);
               database.queries.push_back(Database::Query{now_ms, query_id});
            }
         }

         sort(database.queries.begin(), database.queries.end(), [](const Database::Query &lhs, const Database::Query &rhs) {
            return lhs.start<rhs.start;
         });
      }
   }

   void GenerateQueryArguments()
   {
      mt19937 gen(GetSeedForQueryArguments());

      for (auto &database: databases) {
         TpchQueries::UpdateState update_state(1000);

         //         // Debug code to just create one TPC-H run
         //         database.queries.clear();
         //         for (uint32_t i = 1; i<=23; i++) {
         //            database.queries.push_back({0, i});
         //         }

         for (auto &query : database.queries) {
            query.arguments = TpchQueries::GenerateQueryArguments(query.query_id, database.scale_factor, gen, update_state);
         }
      }
   }

   void DumpDatabasesSizesForR() const
   {
      cout << "db_sizes=c(";
      for (uint64_t idx = 0; idx<databases.size(); idx++) {
         cout << databases[idx].scale_factor * 1_GB << (idx == databases.size() - 1 ? "" : ",");
      }
      cout << ")" << endl;
   }

   void DumpDatabaseCpuTimesForR() const
   {
      cout << "cpu_times=c(";
      for (uint64_t idx = 0; idx<databases.size(); idx++) {
         cout << databases[idx].cpu_time << (idx == databases.size() - 1 ? "" : ",");
      }
      cout << ")" << endl;
   }

   void DumpDatabaseSizeBucketsForR() const
   {
      cout << "size_buckets=c(";
      for (uint64_t idx = 0; idx<databases.size(); idx++) {
         cout << databases[idx].GetSizeBucket() << (idx == databases.size() - 1 ? "" : ",");
      }
      cout << ")" << endl;
   }

   void DumpDatabaseWithPattern() const
   {
      for (uint64_t idx = 0; idx<databases.size(); idx++) {
         cout << idx << ": " << databases[idx].pattern_id << endl;
      }
   }

   void DumpDatabaseQueryArrivalsCsv(ostream &os) const
   {
      os << "db,pattern,time,query" << endl;
      for (auto &database : databases) {
         for (auto &query : database.queries) {
            os << database.database_id << ',' << database.pattern_id << ',' << query.start << ',' << query.query_id << '\n';
         }
      }
      os << flush;
   }
};

// clang++ -std=c++17 -Wall -Werror=return-type -Werror=non-virtual-dtor -Werror=sequence-point -Wsign-compare -march=native -O2 -Wfatal-errors benchmark.cpp
int main(int argc, char **argv)
{
   const uint64_t total_size = 4_TB;
   const uint64_t total_cpu_hours = 40;
   const uint64_t total_duration_in_hours = 1;
   const uint64_t database_count = 20;

   Generator generator;
   generator.GenerateDatabases(database_count, total_size);
   generator.GenerateCpuTimeForDatabases(total_cpu_hours);
   //   generator.GenerateFixedDatabases(database_count, 10, 5);
   generator.GenerateQueryArrivalDistribution();
   generator.GenerateQueryArrivalTimes(total_duration_in_hours);

   generator.DumpDatabaseCpuTimesForR();
   generator.DumpDatabaseSizeBucketsForR();

   generator.GenerateQueryArguments();
   for (uint32_t i = 0; i<generator.databases.size(); i++) {
      ofstream os("query_streams/query_stream_" + to_string(i) + ".json");
      generator.databases[i].WriteJson(os);
   }

   return 0;
}
