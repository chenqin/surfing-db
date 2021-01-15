//
// Created by cq on 1/14/21.
//

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

class XGBOperator {
public:
  std::vector<Field> fields;
  std::unique_ptr<DMatrixHandle> dtrain, dtest, dlabeledTrain;
  std::unique_ptr<BoosterHandle> booster;
  std::string tree_method;
  std::string objective;
  double min_child_weight;
  double gamma;
  uint8_t max_depth;
  bool verbosity;

  XGBOperator(std::vector<Field>& columns) {
    for (const auto& f : columns) {
      CHECK_EQ(f.type, RowType::DOUBLE); //
    }
    fields = columns;
    tree_method = "hist";
    objective = "binary:logistic";
    min_child_weight = 1;
    gamma = 0.1;
    max_depth = 1;
    verbosity = true;
  }
  ~XGBOperator() {
    if (booster) {
      safe_xgboost(XGBoosterFree(*booster.get()));
    }
    if (dtrain) {
      safe_xgboost(XGDMatrixFree(*dtrain.get()));
    }

    if (dlabeledTrain) {
      safe_xgboost(XGDMatrixFree(*dlabeledTrain.get()));
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
  void fillTrainingData(const float* train, const float* label, int row_count, int column_count) {
    CHECK_NOTNULL(train);
    // CHECK_NOTNULL(test);
    CHECK_EQ(column_count, fields.size());
    CHECK_GE(row_count, 1);
    dtrain = std::make_unique<DMatrixHandle>();
    dlabeledTrain = std::make_unique<DMatrixHandle>();

    safe_xgboost(XGDMatrixCreateFromMat(train, row_count, column_count, 0, dtrain.get()));
    safe_xgboost(XGDMatrixCreateFromMat(label, row_count, column_count, NAN, dlabeledTrain.get()));
    safe_xgboost(XGDMatrixSetFloatInfo(*dlabeledTrain.get(), "label", train, row_count));
  }

  void fillParameter() {
    // configure the training
    // available parameters are described here:
    //   https://xgboost.readthedocs.io/en/latest/parameter.html
    safe_xgboost(XGBoosterSetParam(*booster.get(), "tree_method", tree_method.c_str()));
    // avoid evaluating objective and metric on a GPU
    safe_xgboost(XGBoosterSetParam(*booster.get(), "gpu_id", "-1"));

    safe_xgboost(XGBoosterSetParam(*booster.get(), "objective", objective.c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "min_child_weight", std::to_string(min_child_weight).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "gamma", std::to_string(gamma).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "max_depth", std::to_string(max_depth).c_str()));
    safe_xgboost(XGBoosterSetParam(*booster.get(), "verbosity", std::to_string(verbosity).c_str()));
  }

  void train() {
    DMatrixHandle eval_dmats[2] = { *dlabeledTrain.get(), *dlabeledTrain.get() }; // hack
    booster = std::make_unique<BoosterHandle>();
    safe_xgboost(XGBoosterCreate(eval_dmats, 2, booster.get()));

    bst_ulong num_feature = 0;
    safe_xgboost(XGBoosterGetNumFeature(*booster.get(), &num_feature));
    CHECK_EQ(num_feature, features());

    fillParameter();

    // train and evaluate for 10 iterations
    int n_trees = 10;
    const char* eval_names[2] = { "train", "test" };
    const char* eval_result = NULL;
    for (int i = 0; i < n_trees; ++i) {
      safe_xgboost(XGBoosterUpdateOneIter(*booster.get(), i, *dlabeledTrain.get()));
      safe_xgboost(XGBoosterEvalOneIter(*booster.get(), i, eval_dmats, eval_names, 2, &eval_result));
      printf("%s\n", eval_result);
    }
  }

  void predict() {
    CHECK(dtest);

    // predict
    bst_ulong out_len = 0;
    const float* out_result = NULL;
    int n_print = 10;

    safe_xgboost(XGBoosterPredict(*booster.get(), *dtest.get(), 0, 0, 0, &out_len, &out_result));
    printf("y_pred: ");
    for (int i = 0; i < n_print; ++i) {
      printf("%1.4f ", out_result[i]);
    }
    printf("\n");

    // print true labels
    safe_xgboost(XGDMatrixGetFloatInfo(*dtest.get(), "label", &out_len, &out_result));
    printf("y_test: ");
    for (int i = 0; i < n_print; ++i) {
      printf("%1.4f ", out_result[i]);
    }
    printf("\n");
  }
};
} // namespace table
} // namespace surfingdb
#endif //SURFINGDB_XGBOPERATOR_H
