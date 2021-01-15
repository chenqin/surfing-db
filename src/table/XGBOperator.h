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
  std::unique_ptr<DMatrixHandle> dtrain, dtest;
  std::unique_ptr<BoosterHandle> booster;
  std::string tree_method;
  std::string objective;
  double min_child_weight;
  double gamma;
  uint8_t max_depth;
  bool verbosity;

  XGBOperator(std::vector<Field>& columns) {
    for(const auto& f : columns) {
      CHECK_EQ(f.type, RowType::DOUBLE); //
    }
    fields = columns;
    tree_method = "hist";
    objective = "binary:logistic";
    min_child_weight = 1;
    gamma = 0.1;
    max_depth = 3;
    verbosity = true;
  }
  ~XGBOperator() {
    if(booster) {
      safe_xgboost(XGBoosterFree(*booster.get()));
    }
    if(dtrain) {
      safe_xgboost(XGDMatrixFree(*dtrain.get()));
    }
    if(dtest) {
      safe_xgboost(XGDMatrixFree(*dtest.get()));
    }
  }

  /**
   * we use naive dense float array to build DMatrix for now
   * @param train
   * @param test
   * @param row_count
   * @param column_count
   */
  void fillData(const float* train, const float* test, int row_count, int column_count){
    CHECK_NOTNULL(train);
    CHECK_NOTNULL(test);
    CHECK_EQ(column_count, fields.size());
    CHECK_GE(row_count, 1);
    dtrain = std::make_unique<DMatrixHandle>();
    dtest = std::make_unique<DMatrixHandle>();
    safe_xgboost(XGDMatrixCreateFromMat(train, row_count, column_count, 0, dtrain.get()));
    safe_xgboost(XGDMatrixCreateFromMat(test, row_count, column_count, 0, dtest.get()));
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
    DMatrixHandle eval_dmats[2] = { *dtrain.get(), *dtest.get() };
    booster = std::make_unique<BoosterHandle>();
    safe_xgboost(XGBoosterCreate(eval_dmats, 2, booster.get()));

    fillParameter();

    // train and evaluate for 10 iterations
    int n_trees = 10;
    const char* eval_names[2] = { "train", "test" };
    const char* eval_result = NULL;
    for (int i = 0; i < n_trees; ++i) {
      safe_xgboost(XGBoosterUpdateOneIter(*booster.get(), i, *dtrain.get()));
      safe_xgboost(XGBoosterEvalOneIter(*booster.get(), i, eval_dmats, eval_names, 2, &eval_result));
      printf("%s\n", eval_result);
    }

    bst_ulong num_feature = 0;
    safe_xgboost(XGBoosterGetNumFeature(*booster.get(), &num_feature));
    printf("num_feature: %lu\n", (unsigned long)(num_feature));
  }

  void predict() {
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
