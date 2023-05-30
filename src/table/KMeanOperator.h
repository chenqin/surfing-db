/*
 * Copyright Chen Qin on 12/30/20.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef MATCHA_KMEANOPERATOR_H
#define MATCHA_KMEANOPERATOR_H

#include "mrow.h"

namespace matcha {
namespace table {

template <class BidiIter>
BidiIter random_unique(BidiIter begin, BidiIter end, size_t num_random) {
  size_t left = std::distance(begin, end);
  while (num_random--) {
    BidiIter r = begin;
    std::advance(r, rand() % left);
    std::swap(*begin, *r);
    ++begin;
    --left;
  }
  return begin;
}

class KMeanOperator {
 public:
  int k;
  std::vector<Field> fields;
  size_t max_iteration;
  size_t iteration;
  std::unordered_map<int, std::set<size_t>> groups;
  DOUBLE_TYPE* centers;
  bool inited;

  KMeanOperator(int k, const std::vector<Field> fields, size_t max_iteration) {
    CHECK_GE(k, 1);
    this->k = k;
    CHECK_GT(fields.size(), 0);
    for (auto f : fields) {
      // CHECK(schema_ptr->exist(f));
      CHECK(f.type == RowType::DOUBLE);  // force normalization before using
    }
    this->fields = fields;
    for (int i = 0; i < this->k; i++) {
      groups.insert({i, std::set<size_t>()});
    }
    this->max_iteration = max_iteration;
    iteration = 0;
    centers = (DOUBLE_TYPE*)malloc(sizeof(double) * k * fields.size());
    memset(centers, 0, sizeof(DOUBLE_TYPE) * k * fields.size());
    inited = false;
  }

  ~KMeanOperator() { free(centers); }

  void init(size_t count, int rank, int world,
            std::unordered_map<int, size_t>& local_picks) {
    // step 1: figure out all rows in all processes
    size_t data_size = 0;
    MPI_Allreduce(&count, &data_size, 1, MPI_UNSIGNED_LONG, MPI_SUM,
                  MPI_COMM_WORLD);
    size_t local_data_size[world], recv[world];
    memset(&local_data_size[0], 0, sizeof(size_t));
    memset(&recv[0], 0, sizeof(size_t));
    local_data_size[rank] = count;
    MPI_Allreduce(&local_data_size[0], &recv[0], world, MPI_UNSIGNED_LONG,
                  MPI_MAX, MPI_COMM_WORLD);

    // step 2: random pick k as centriod
    std::vector<size_t> offsets(data_size);
    if (rank == 0) {
      for (size_t i = 0; i < data_size; i++) {
        offsets.at(i) = i;
      }
      random_unique(offsets.begin(), offsets.end(), k);
    }
    std::vector<size_t> centriod(data_size);
    centriod.resize(k);
    // step 3, send cetriods to all nodes
    MPI_Allreduce(&offsets[0], &centriod[0], k, MPI_UNSIGNED_LONG, MPI_MAX,
                  MPI_COMM_WORLD);
    size_t local_start_index = 0, local_end_index;
    for (int i = 0; i <= rank - 1; i++) {
      local_start_index += recv[i];
    }
    local_end_index = local_start_index + count;

    int j = 0;
    for (auto item : centriod) {
      if (item >= local_start_index && item < local_end_index) {
        auto pick_index = item - local_start_index;
        local_picks.insert({j, pick_index});
      }
      j++;
    }
  }

  void addGroup(int i, size_t index) { groups.at(i).insert(index); }

  bool shouldStop() { return iteration >= max_iteration; }

  void process(mrow, mrow, mrow) {}
};
}  // namespace table
}  // namespace matcha
#endif  // MATCHA_KMEANOPERATOR_H
