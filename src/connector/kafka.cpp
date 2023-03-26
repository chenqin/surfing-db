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
#include "kafka.h"
#include <chrono>
#include <csignal>
#include <ctype.h>
#include <iostream>
#include <fstream>
#include <experimental/random>
#include <glog/logging.h>
#include <mpi.h>
#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <vector>

namespace surfingdb {
namespace connector {

volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
void stop(int sig) {
  run = 0;
}

bool issubscriber;

int exit_eof = 0;
int wait_eof = 0; /* number of partitions awaiting EOF */

void KafkaConnector::print_partition_list(const rd_kafka_topic_partition_list_t* partitions) {
  for (int i = 0; i < partitions->cnt; i++) {
    LOG(INFO) << "topic " << partitions->elems[i].topic << " partition " << partitions->elems[i].partition << " offset " << partitions->elems[i].offset;
  }
}

std::string serversettobrokers(std::string serversetpath) {
  std::string line;
  std::string brokers = "";
  int count = 0;
  int start = std::experimental::randint(1, 20);
  std::ifstream myfile(serversetpath.c_str());
  if (myfile.is_open())
  {
    while ( getline (myfile,line) && count < 1)
    {
      //std::cout << line << '\n';
      if(start-- < 0){
        brokers += line + ",";
        count++;
      }
    }
    myfile.close();
  }

  else std::cout << "Unable to open file"; 
  return brokers.substr(0, brokers.length() - 1);
}

KafkaConnector::KafkaConnector(const std::shared_ptr<node> node_ptr,
                               std::string connector_name,
                               size_t max_batch_size,
                               int timeout,
                               std::shared_ptr<mschema> schema_ptr,
                               std::function<std::shared_ptr<mrow>(const char* payload, const mschema& schema)> deser,
                               std::vector<std::string> topicvect, std::string serversetpath, std::string groupid, bool pii=false) : Connector(node_ptr,
                                                                                                        connector_name,
                                                                                                        max_batch_size,
                                                                                                        timeout,
                                                                                                        schema_ptr,
                                                                                                        deser) {
  assigned = false;
  this->serversetpath = serversetpath;
  this->groupid = (char*)groupid.c_str();
  std::string topic_str;
  for(int i = 0 ; i < topicvect.size() ; i++) {
      topic_str += topicvect.at(i) + ",";
  }
  std::string str = topic_str.substr(0, topic_str.length() -1);
  topics = const_cast<char*>(str.c_str());

  topic_cnt = topicvect.size();
  
  conf = rd_kafka_conf_new();
  topic_conf = rd_kafka_topic_conf_new();

  /* Set bootstrap broker(s) as a comma-separated list of
   * host or host:port (default port 9092).
   * librdkafka will use the bootstrap brokers to acquire the full
   * set of brokers from the cluster. */
  if (rd_kafka_conf_set(conf, "bootstrap.servers", (char*) serversettobrokers(serversetpath).c_str(),
                        errstr, sizeof(errstr))
      != RD_KAFKA_CONF_OK) {
    LOG(ERROR) << errstr;
    rd_kafka_conf_destroy(conf);
    return;
  }

  /**
   * @brief set linger.ms
   *
   */
  if (rd_kafka_conf_set(conf, "linger.ms", "120",
                        errstr, sizeof(errstr))
      != RD_KAFKA_CONF_OK) {
    LOG(ERROR) << errstr;
    rd_kafka_conf_destroy(conf);
    return;
  }

  /* Set the consumer group id.
   * All consumers sharing the same group id will join the same
   * group, and the subscribed topic' partitions will be assigned
   * according to the partition.assignment.strategy
   * (consumer config property) to the consumers in the group. */
  if (rd_kafka_conf_set(conf, "group.id", this->groupid,
                        errstr, sizeof(errstr))
      != RD_KAFKA_CONF_OK) {
    LOG(ERROR) << errstr;
    rd_kafka_conf_destroy(conf);
    return;
  }
  if(pii) {
    /**
    'group.id': consumer_group,
    'auto.offset.reset': 'earliest',
    'max.partition.fetch.bytes': 1024**2,
    'security.protocol': 'ssl',
    'ssl.key.location': '/var/lib/normandie/fuse/key/generic',
    'ssl.certificate.location': '/var/lib/normandie/fuse/chain/generic',
    'ssl.ca.location': '/var/lib/normandie/fuse/ca/root',
    'enable.sparse.connections': False,
    'error_cb': self.error_cb,
    */
    if (rd_kafka_conf_set(conf, "security.protocol", "ssl",
                          errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
      LOG(ERROR) << errstr;
      rd_kafka_conf_destroy(conf);
      return;
    }
    if (rd_kafka_conf_set(conf, "ssl.key.location", "/var/lib/normandie/fuse/key/generic",
                          errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
      LOG(ERROR) << errstr;
      rd_kafka_conf_destroy(conf);
      return;
    }
    if (rd_kafka_conf_set(conf, "ssl.certificate.location", "/var/lib/normandie/fuse/chain/generic",
                          errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
      LOG(ERROR) << errstr;
      rd_kafka_conf_destroy(conf);
      return;
    }
    if (rd_kafka_conf_set(conf, "ssl.ca.location", "/var/lib/normandie/fuse/ca/root",
                          errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
      LOG(ERROR) << errstr;
      rd_kafka_conf_destroy(conf);
      return;
    }
    if (rd_kafka_conf_set(conf, "debug", "generic,broker,topic,metadata,feature,queue,msg,protocol,cgrp,security,fetch,interceptor,plugin,consumer,admin,eos,mock,all",
                          errstr, sizeof(errstr))
        != RD_KAFKA_CONF_OK) {
      LOG(ERROR) << errstr;
      rd_kafka_conf_destroy(conf);
      return;
    }
  }

  /* Set default topic config for pattern-matched topics. */
  rd_kafka_conf_set_default_topic_conf(conf, topic_conf);
  auto cb = [](rd_kafka_t* rk, rd_kafka_resp_err_t err,
    rd_kafka_topic_partition_list_t* partitions,
    void* opaque) {
        rd_kafka_error_t* error = NULL;
        rd_kafka_resp_err_t ret_err = RD_KAFKA_RESP_ERR_NO_ERROR;

        fprintf(stderr, "%% Consumer group rebalanced: ");

        switch (err) {
          case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            fprintf(stderr, "assigned (%s):\n",
                    rd_kafka_rebalance_protocol(rk));
            KafkaConnector::print_partition_list(partitions);

            if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE"))
              error = rd_kafka_incremental_assign(rk, partitions);
            else
              ret_err = rd_kafka_assign(rk, partitions);
            wait_eof += partitions->cnt;
            break;

          case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            fprintf(stderr, "revoked (%s):\n",
                    rd_kafka_rebalance_protocol(rk));
            KafkaConnector::print_partition_list(partitions);
            if (!strcmp(rd_kafka_rebalance_protocol(rk), "COOPERATIVE")) {
              error = rd_kafka_incremental_unassign(rk, partitions);
              wait_eof -= partitions->cnt;
            } else {
              ret_err = rd_kafka_assign(rk, NULL);
              wait_eof = 0;
            }
            break;

          default:
            fprintf(stderr, "failed: %s\n",
                    rd_kafka_err2str(err));
            rd_kafka_assign(rk, NULL);
            break;
        }

        if (error) {
          fprintf(stderr, "incremental assign failure: %s\n",
                  rd_kafka_error_string(error));
          rd_kafka_error_destroy(error);
        } else if (ret_err) {
          fprintf(stderr, "assign failure: %s\n",
                  rd_kafka_err2str(ret_err));
        }
    };
  /* Callback called on partition assignment changes */
  rd_kafka_conf_set_rebalance_cb(conf, cb);

  rd_kafka_conf_set(conf, "enable.partition.eof", "false",
                    NULL, 0);

  /* If there is no previously committed offset for a partition
   * the auto.offset.reset strategy will be used to decide where
   * in the partition to start fetching messages.
   * By setting this to earliest the consumer will read all messages
   * in the partition if there was no previously committed offset. */
  if (rd_kafka_conf_set(conf, "auto.offset.reset", "earliest",
                        errstr, sizeof(errstr))
      != RD_KAFKA_CONF_OK) {
    LOG(ERROR) << errstr;
    rd_kafka_conf_destroy(conf);
    return;
  }

  /*
   * Create consumer instance.
   *
   * NOTE: rd_kafka_new() takes ownership of the conf object
   *       and the application must not reference it again after
   *       this call.
   */
  rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf, errstr, sizeof(errstr));
  if (!rk) {
    LOG(ERROR) << errstr;
    return;
  }

  conf = NULL; /* Configuration object is now owned, and freed,
                * by the rd_kafka_t instance. */

  /* Redirect all messages from per-partition queues to
   * the main queue so that messages can be consumed with one
   * call from all assigned partitions.
   *
   * The alternative is to poll the main queue (for events)
   * and each partition queue separately, which requires setting
   * up a rebalance callback and keeping track of the assignment:
   * but that is more complex and typically not recommended. */
  rd_kafka_poll_set_consumer(rk);

  /* Convert the list of topics to a format suitable for librdkafka */
  subscription = rd_kafka_topic_partition_list_new(topic_cnt);

  rd_kafka_topic_partition_list_add(subscription,
                                    topics,
                                    /* the partition is ignored
                                     * by subscribe() */
                                    RD_KAFKA_PARTITION_UA);

  /* Subscribe to the list of topics */
  err = rd_kafka_subscribe(rk, subscription);

  if (err) {
    LOG(ERROR) << "Failed to subscribe to " << subscription->cnt << " topics: " << rd_kafka_err2str(err);
    rd_kafka_topic_partition_list_destroy(subscription);
    rd_kafka_destroy(rk);
  }

  LOG(INFO) << "Subscribed to " << subscription->cnt << "topic(s), " << "waiting for rebalance and messages...";

  rd_kafka_topic_partition_list_destroy(subscription);
  signal(SIGINT, stop);
}

KafkaConnector::~KafkaConnector() {
  /* Close the consumer: commit final offsets and leave the group. */
  LOG(INFO) << "Consumer close";
  rd_kafka_consumer_close(rk);

  /* Destroy the consumer */
  rd_kafka_destroy(rk);
}

std::shared_ptr<mtable> KafkaConnector::consume_batch() {
  auto t = std::make_shared<mtable>(node_ptr, schema_ptr, max_batch_size * schema_ptr->rowSize());
  auto start = MPI_Wtime();
  int total = 0;
  while ((MPI_Wtime() - start) * 1000 < timeout && total++ < max_batch_size) {
    rd_kafka_message_t* rkm = rd_kafka_consumer_poll(rk, 50);
    if (!rkm)
      continue; /* Timeout: no message within 100ms,
                 *  try again. This short timeout allows
                 *  checking for `run` at frequent intervals.
                 */

    /* consumer_poll() will return either a proper message
     * or a consumer error (rkm->err is set). */
    if (rkm->err) {
      /* Consumer errors are generally to be considered
       * informational as the consumer will automatically
       * try to recover from all types of errors. */
      LOG(ERROR) << "Consumer error: " << rd_kafka_message_errstr(rkm);
      rd_kafka_message_destroy(rkm);
      continue;
    }
    auto b = deser((const char*)rkm->payload, *schema_ptr.get());
    if (b != nullptr) {
      t->appendRow(*b.get());
    }
    rd_kafka_message_destroy(rkm);
  }
  /**
   * @brief transform table to arrow table and release memory
   *
   */
  return t;
}
} // namespace connector
} // namespace surfingdb