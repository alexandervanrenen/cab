# Cloud Analytics Benchmark (CAB)

A short description of the individual folders and how to use them.

#### ``cab/benchmark-gen``
A c++ program to generate the query streams. Can be configured by changing the variables in the ``main`` function:
```C++
   const uint64_t total_size = 4_TB;
   const uint64_t total_cpu_hours = 40;
   const uint64_t total_duration_in_hours = 1;
   const uint64_t database_count = 20;
```
The output is written to ``benchmark-gen/query_streams``.
It can be compiled with:
```bash
clang++ -std=c++17 -Wall -Werror=return-type -Werror=non-virtual-dtor -Werror=sequence-point -Wsign-compare -march=native -O2 -Wfatal-errors benchmark.cpp
```
Note that the distributions in c++ are platform dependent, hence the generated query streams might look different depending on the platform.
However, the overall distribution/pattern is the same.

### ``cab/benchmark-query-streams``
Contains pre generated query streams.
These are the ones we used in the experiments in the paper.

### ``cab/benchmark-results``
Contains the results we obtained by running the experiments as described in the paper.

### ``cab/benchmark-run``
A set of java script programs for running and analyzing the benchmark results.

### ``cab/snowset-analysis``
All R scripts we used to analyze the snowset and to plot the results of the benchmark run.
