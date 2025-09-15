// Generate larger Thrift payloads for benchmarking: includes sizeable strings and maps
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <fstream>
#include <iostream>
#include <random>
#include <string>
#include <vector>
#include "mabs/mabs_types.h"

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TMemoryBuffer;

static std::string random_ascii(size_t n, std::mt19937& rng) {
  static const char table[] = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-";
  std::uniform_int_distribution<int> dist(0, (int)(sizeof(table) - 2));
  std::string s; s.resize(n);
  for (size_t i = 0; i < n; ++i) s[i] = table[dist(rng)];
  return s;
}

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <out_file> [rows] [str_len] [map_entries] [hist_len]\n";
    return 2;
  }
  std::string out_path = argv[1];
  int rows = (argc >= 3) ? std::stoi(argv[2]) : 100000; // default 100k
  int str_len = (argc >= 4) ? std::stoi(argv[3]) : 256;  // per-string length
  int map_entries = (argc >= 5) ? std::stoi(argv[4]) : 16; // entries per map
  int hist_len = (argc >= 6) ? std::stoi(argv[5]) : 512; // histogram string length

  std::ofstream out(out_path, std::ios::binary);
  if (!out) { std::cerr << "Failed to open output: " << out_path << std::endl; return 1; }

  std::mt19937 rng(12345);

  for (int i = 0; i < rows; ++i) {
    MabsMetrics m;
    m.timestamp = static_cast<int64_t>(i);
    m.__set_service_tags(random_ascii((size_t)str_len, rng));
    m.__set_node_tags(random_ascii((size_t)str_len, rng));
    m.__set_service_name(random_ascii((size_t)str_len / 2, rng));

    // counters: map<string, i64>
    {
      std::map<std::string, int64_t> counters;
      for (int k = 0; k < map_entries; ++k) counters["ctr_" + std::to_string(k)] = (int64_t)(i * 100 + k);
      m.__set_counters(counters);
    }
    // gauges: map<string, double>
    {
      std::map<std::string, double> gauges;
      for (int k = 0; k < map_entries; ++k) gauges["g_" + std::to_string(k)] = (double)k + 0.5;
      m.__set_gauges(gauges);
    }
    // double_counters: map<string, double>
    {
      std::map<std::string, double> dcs;
      for (int k = 0; k < map_entries; ++k) dcs["dc_" + std::to_string(k)] = (double)i * 0.1 + k;
      m.__set_double_counters(dcs);
    }
    // histograms: map<string, string>
    {
      std::map<std::string, std::string> hists;
      for (int k = 0; k < map_entries / 2; ++k) hists["h_" + std::to_string(k)] = random_ascii((size_t)hist_len, rng);
      m.__set_histograms(hists);
    }

    auto mem = std::make_shared<TMemoryBuffer>();
    TBinaryProtocol proto(mem);
    m.write(&proto);

    uint8_t* buf = nullptr; uint32_t sz = 0;
    mem->getBuffer(&buf, &sz);
    uint32_t le_sz = sz;
    out.write(reinterpret_cast<const char*>(&le_sz), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(buf), sz);
  }
  std::cerr << "Wrote " << rows << " large payloads to " << out_path << std::endl;
  return 0;
}
