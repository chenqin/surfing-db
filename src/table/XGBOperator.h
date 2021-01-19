//
// Created by cq on 1/14/21.
//

#include <glog/logging.h>
#include <xgboost/c_api.h>
#include "Node.h"
#include "row.h"
#ifndef SURFINGDB_XGBOPERATOR_H
#define SURFINGDB_XGBOPERATOR_H

namespace surfingdb {
namespace table {

#define safe_xgboost(call)                                                                       \
  {                                                                                              \
    int err = (call);                                                                            \
    if (err != 0) {                                                                              \
      fprintf(stderr, "%s:%d: error in %s: %s\n", __FILE__, __LINE__, #call, XGBGetLastError()); \
      exit(1);                                                                                   \
    }                                                                                            \
  }

struct XGBParameters {
  std::string tree_method;
  std::string objective;
  std::string eval_metric;
  double min_child_weight;
  double gamma;
  uint8_t max_depth;
  bool verbosity;
  int root = 0;
  bool isTraining = true;
};

class XGBOperator {
public:
  std::vector<Field> fields;
  Field labelField;
  XGBParameters parameters;
  int rank, world;
  std::string url;
  std::unique_ptr<DMatrixHandle> dtrain, dtest;
  std::unique_ptr<BoosterHandle> booster;

  XGBOperator(const std::vector<Field>& features, const Field& label, const XGBParameters& parameters1, int rank, int world) : fields(features), labelField(label), parameters(parameters1), rank(rank), world(world) {
    for (const auto& f : features) {
      CHECK_EQ(f.type, RowType::DOUBLE); //thrift don't support float
    }
    url =  "/tmp/test.bin";
  }
  ~XGBOperator() {
    if (booster) {
      safe_xgboost(XGBoosterFree(*booster.get()));
    }
    if (dtrain) {
      safe_xgboost(XGDMatrixFree(*dtrain.get()));
    }
    if (dtest) {
      safe_xgboost(XGDMatrixFree(*dtest.get()));
    }
  }

  size_t features() {
    return fields.size();
  }

  /**
   * we use naive dense float array to build DMatrix for now
   * @param train float array point to training dataset without label row*column
   * @param label labels of each training dataset row
   * @param row_count training dataset rows
   * @param column_count features number
   */
  void train(const float* train, const float* label, bst_ulong row_count, int feature_num) {
    // if(rank != parameters.root) return; // only train on root process
    LOG(INFO) << "total training dataset at root " << row_count << " rows";
    CHECK_NOTNULL(train);
    // CHECK_NOTNULL(test);
    CHECK_EQ(feature_num, fields.size());
    CHECK_GE(row_count, 1);
    dtrain = std::make_unique<DMatrixHandle>();

    // set row_count of features adding label column
    safe_xgboost(XGDMatrixCreateFromMat(train, row_count, feature_num, -1, dtrain.get()));
    safe_xgboost(XGDMatrixSetFloatInfo(*dtrain.get(), "label", label, row_count));

    DMatrixHandle eval_dmats[2] = { *dtrain.get(), *dtrain.get() }; // hack
    booster = std::make_unique<BoosterHandle>();
    safe_xgboost(XGBoosterCreate(eval_dmats, 2, booster.get()));

    bst_ulong num_feature = 0;
    safe_xgboost(XGBoosterGetNumFeature(*booster.get(), &num_feature));
    CHECK_EQ(num_feature, features());

    fillParameter();

    if (rank != parameters.root) return; // only run train steps on root

    // train and evaluate for 10 iterations
    int n_trees = 10;
    const char* eval_names[2] = { "train", "test" };
    const char* eval_result = NULL;
    for (int i = 0; i < n_trees; ++i) {
      safe_xgboost(XGBoosterUpdateOneIter(*booster.get(), i, *dtrain.get()));
      safe_xgboost(XGBoosterEvalOneIter(*booster.get(), i, eval_dmats, eval_names, 2, &eval_result));
      printf("%s\n", eval_result);
    }
  }

  void fillParameter() {
    // configure the training
    // available parameters are described here:
    //   https://xgboost.readthedocs.io/en/latest/parameter.html
    safe_xgboost(XGBoosterSetParam(*booster.get(), "tree_method", parameters.tree_method.c_str()));
    // avoid evaluating objective and metric on a GPU
    safe_xgboost(XGBoosterSetParam(*booster.get(), "gpu_id", "-1"));

    safe_xgboost(XGBoosterSetParam(*booster.get(), "objective", parameters.objective.c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "min_child_weight", std::to_string(parameters.min_child_weight).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "gamma", std::to_string(parameters.gamma).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "max_depth", std::to_string(parameters.max_depth).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "eval_metric", parameters.eval_metric.c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "verbosity", std::to_string(parameters.verbosity).c_str()));
  }

  void gather(const float* train, const float* label, size_t& row_count, const int column_count) {
    int max_rank;
    MPI_Allreduce(&rank, &max_rank, 1, MPI_INT, MPI_MAX, MPI_COMM_WORLD);
    CHECK_GT(max_rank, 0); // at least two processes
    int myaligns[world], rcounts[world], displs[world];
    memset(myaligns, 0, sizeof(int) * world);
    memset(rcounts, 0, sizeof(int) * world);
    memset(displs, 0, sizeof(int) * world);

    myaligns[rank] = row_count * column_count;
    MPI_Reduce(myaligns, rcounts, world, MPI_INT, MPI_SUM, parameters.root, MPI_COMM_WORLD);

    int offset = 0;
    int total_row_count = 0;
    for (int i = 0; i < world; i++) {
      int temp = rcounts[i];
      total_row_count += rcounts[i] / column_count;
      displs[i] = offset;
      offset += temp;
    }

    if (rank == parameters.root) {
      float train_all[total_row_count];
      float label_all[total_row_count];
      MPI_Gatherv(train, row_count * column_count, MPI_FLOAT, train_all, rcounts,
                  displs, MPI_FLOAT,
                  parameters.root, MPI_COMM_WORLD);
      MPI_Gatherv(label, row_count, MPI_FLOAT, label_all, rcounts,
                  displs, MPI_FLOAT,
                  parameters.root, MPI_COMM_WORLD);
      train = train_all;
      label = label_all;
      row_count = total_row_count; //update row_count
    } else {
      MPI_Gatherv(train, row_count * column_count, MPI_FLOAT, nullptr, nullptr,
                  nullptr, MPI_FLOAT,
                  parameters.root, MPI_COMM_WORLD);
      MPI_Gatherv(label, row_count, MPI_FLOAT, nullptr, nullptr,
                  nullptr, MPI_FLOAT,
                  parameters.root, MPI_COMM_WORLD);
    }
  }
  /**
   * need to fix broadcast model to all
   * TODO(chenqin): (fix double free buffer)
   */
  void syncModel() {
    const char* buffer = static_cast<char*>(malloc(HUGE_PAGE_SIZE));
    bst_ulong model_len;
    if(rank == parameters.root) {
      safe_xgboost(XGBoosterSaveModel(*booster.get(), url.c_str()));
      safe_xgboost(XGBoosterSaveJsonConfig(*booster.get(), &model_len, &buffer));
    }
    MPI_Bcast(&model_len, 1, MPI_UNSIGNED_LONG, parameters.root, MPI_COMM_WORLD);
    CHECK_GT(model_len, 0);
    char* model = static_cast<char*>(malloc(model_len));
    if(rank == parameters.root) {
      memcpy((void*)model, buffer, model_len);
    } else {
      memset((void*) model, 0, model_len);
    }
    CHECK_NOTNULL(buffer);
    MPI_Bcast((void*)model, model_len, MPI_CHAR, parameters.root, MPI_COMM_WORLD);
    CHECK_NOTNULL(model);
    safe_xgboost(XGBoosterLoadJsonConfig(*booster.get(), model));
  }

  void predict(const float* test, const float* result, const bst_ulong row_count, int feature_num) {
    dtest = std::make_unique<DMatrixHandle>();
    safe_xgboost(XGDMatrixCreateFromMat(test, row_count, feature_num, -1, dtest.get()));
    bst_ulong out_come = 0;
    safe_xgboost(XGBoosterPredict(*booster.get(), *dtest.get(), 0, 0, 0, &out_come, &result));
    CHECK_EQ(row_count, out_come);
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_XGBOPERATOR_H
