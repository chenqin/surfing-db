//
// Created by Chen Qin on 12/31/20.
//
#include <chrono>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <kll_sketch.hpp>
#include <random>
#include "frequent_items_sketch.hpp"
#include "table/Operator.h"
#include "table/table.h"

namespace surfingdb {
namespace table {
namespace test {
struct myDummy {
  int a;
};
TEST(TableTest, TestPartitionAloha) {
  std::vector<myDummy> dataIn, dataInR, dataOut;
  std::function<int(const int&, const int&, const myDummy&)> partitioner =
    [=](const int& rank, const int& world, const myDummy& s) { return (s.a + rank) % world; };
  PartitionOp<myDummy> op1(0, 1, MPI_INT, partitioner);
}

TEST(TableTest, TestPadDoAloha) {
  std::vector<myDummy> dataIn, dataInR;
  std::vector<int> sum;
  std::function<void(const myDummy&, const myDummy&, int&)> doFunc =
    [=](const myDummy& l, const myDummy& r, int& out) { out = l.a + r.a; };
  ParDoOp<myDummy, myDummy, int> op1(doFunc);
}

TEST(TableTest, TestSketchFrequency) {
  typedef datasketches::frequent_items_sketch<std::string> frequent_strings_sketch;

  // this section generates two sketches and serializes them into files
  {
    frequent_strings_sketch sketch1(64);
    sketch1.update("a");
    sketch1.update("a");
    sketch1.update("b");
    sketch1.update("c");
    sketch1.update("a");
    sketch1.update("d");
    sketch1.update("a");
    std::ofstream os1("freq_str_sketch1.bin");
    sketch1.serialize(os1);

    frequent_strings_sketch sketch2(64);
    sketch2.update("e");
    sketch2.update("a");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("f");
    sketch2.update("g");
    sketch2.update("a");
    sketch2.update("f");
    std::ofstream os2("freq_str_sketch2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints the result
  {
    std::ifstream is1("freq_str_sketch1.bin");
    frequent_strings_sketch sketch1 = frequent_strings_sketch::deserialize(is1);

    std::ifstream is2("freq_str_sketch2.bin");
    frequent_strings_sketch sketch2 = frequent_strings_sketch::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the original sketches intact
    frequent_strings_sketch u(64);
    u.merge(sketch1);
    u.merge(sketch2);

    auto items = u.get_frequent_items(datasketches::NO_FALSE_POSITIVES);
    std::cout << "Frequent strings: " << items.size() << std::endl;
    std::cout << "Str\tEst\tLB\tUB" << std::endl;
    for (auto row : items) {
      std::cout << row.get_item() << "\t" << row.get_estimate() << "\t"
                << row.get_lower_bound() << "\t" << row.get_upper_bound() << std::endl;
    }
  }
}

TEST(TableTest, TestSketchQuantile) {
  // this section generates two sketches from random data and serializes them into files
  {
    std::default_random_engine generator(std::chrono::system_clock::now().time_since_epoch().count());
    std::normal_distribution<float> nd(0, 1); // mean=0, stddev=1

    datasketches::kll_sketch<float> sketch1; // default k=200
    for (int i = 0; i < 10000; i++) {
      sketch1.update(nd(generator)); // mean=0, stddev=1
    }
    std::ofstream os1("kll_sketch_float1.bin");
    sketch1.serialize(os1);

    datasketches::kll_sketch<float> sketch2; // default k=200
    for (int i = 0; i < 10000; i++) {
      sketch2.update(nd(generator) + 1); // shift the mean for the second sketch
    }
    std::ofstream os2("kll_sketch_float2.bin");
    sketch2.serialize(os2);
  }

  // this section deserializes the sketches, produces a union and prints some results
  {
    std::ifstream is1("kll_sketch_float1.bin");
    auto sketch1 = datasketches::kll_sketch<float>::deserialize(is1);

    std::ifstream is2("kll_sketch_float2.bin");
    auto sketch2 = datasketches::kll_sketch<float>::deserialize(is2);

    // we could merge sketch2 into sketch1 or the other way around
    // this is an example of using a new sketch as a union and keeping the original sketches intact
    datasketches::kll_sketch<float> u; // default k=200
    u.merge(sketch1);
    u.merge(sketch2);

    // Debug output
    // u.to_stream(std::cout);

    std::cout << "Min, Median, Max values" << std::endl;
    const double fractions[3]{ 0, 0.5, 1 };
    auto quantiles = u.get_quantiles(fractions, 3);
    std::cout << quantiles[0] << ", " << quantiles[1] << ", " << quantiles[2] << std::endl;

    std::cout << "Probability Histogram: estimated probability mass in 4 bins: (-inf, -2), [-2, 0), [0, 2), [2, +inf)" << std::endl;
    const float split_points[]{ -2, 0, 2 };
    const int num_split_points = 3;
    auto pmf = u.get_PMF(split_points, num_split_points);
    std::cout << pmf[0] << ", " << pmf[1] << ", " << pmf[2] << ", " << pmf[3] << std::endl;

    std::cout << "Frequency Histogram: estimated number of original values in the same bins" << std::endl;
    const int num_bins = num_split_points + 1;
    int histogram[num_bins];
    for (int i = 0; i < num_bins; i++) {
      histogram[i] = pmf[i] * u.get_n(); // scale the fractions by the total count of values
    }
    std::cout << histogram[0] << ", " << histogram[1] << ", " << histogram[2] << ", " << histogram[3] << std::endl;
  }
}
} // namespace test
} // namespace table
} // namespace surfingdb