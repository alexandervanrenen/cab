#pragma once

#include <unistd.h>
#include <cstdint>
#include <iostream>

// Byte suffixes
auto operator ""_B(unsigned long long x) -> uint64_t { return x; }
auto operator ""_KB(unsigned long long x) -> uint64_t { return x * 1000ull; }
auto operator ""_MB(unsigned long long x) -> uint64_t { return x * 1000ull * 1000ull; }
auto operator ""_GB(unsigned long long x) -> uint64_t { return x * 1000ull * 1000ull * 1000ull; }
auto operator ""_TB(unsigned long long x) -> uint64_t { return x * 1000ull * 1000ull * 1000ull * 1000ull; }

// Time suffixes (all time in seconds)
auto operator ""_ms(unsigned long long x) -> double { return x / 1000.0; }
auto operator ""_sec(unsigned long long x) -> double { return x; }
auto operator ""_min(unsigned long long x) -> double { return x * 60.0; }
auto operator ""_hour(unsigned long long x) -> double { return x * 3600.0; }
auto operator ""_day(unsigned long long x) -> double { return x * 86400.0; }

// 'ensure' is always there for you (like assert but not disabled when NDEBUG=1)
void OnEnsureFailedPrint(const char *func, const char *file, int line, const char *expression)
{
   std::cerr << "Ensure failed: (" << expression << "), function " << func << ", file " << file << ":" << line << std::endl;
   throw;
}
#define unlikely(expr)  (__builtin_expect(!!(expr), 0))
#define ensure(e) \
    (unlikely(!(e)) ? OnEnsureFailedPrint(__func__, __FILE__, __LINE__, #e) : (void)0)
