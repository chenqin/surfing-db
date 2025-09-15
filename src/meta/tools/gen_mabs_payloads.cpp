#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include "mabs/mabs_types.h"

using apache::thrift::protocol::TBinaryProtocol;
using apache::thrift::transport::TMemoryBuffer;

int main(int argc, char** argv) {
  if (argc < 2) {
    std::cerr << "Usage: " << argv[0] << " <out_file> [rows]" << std::endl;
    return 2;
  }
  std::string out_path = argv[1];
  int rows = (argc >= 3) ? std::stoi(argv[2]) : 200000; // default 200k
  std::ofstream out(out_path, std::ios::binary);
  if (!out) { std::cerr << "Failed to open output: " << out_path << std::endl; return 1; }

  for (int i = 0; i < rows; ++i) {
    MabsMetrics m;
    m.timestamp = static_cast<int64_t>(i);
    m.service_tags = "svc";
    m.node_tags = "node";
    m.service_name = "svcname";
    // Keep maps empty to focus on primitive/string decode path

    auto mem = std::make_shared<TMemoryBuffer>();
    TBinaryProtocol proto(mem);
    m.write(&proto);

    uint8_t* buf = nullptr; uint32_t sz = 0;
    mem->getBuffer(&buf, &sz);
    uint32_t le_sz = sz;
    out.write(reinterpret_cast<const char*>(&le_sz), sizeof(uint32_t));
    out.write(reinterpret_cast<const char*>(buf), sz);
  }
  std::cerr << "Wrote " << rows << " payloads to " << out_path << std::endl;
  return 0;
}

