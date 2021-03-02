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
#include <mpi.h>
#include <glog/logging.h>
#include <sys/time.h>
#include <csignal>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <vector>
#include <chrono>
#include <librdkafka/rdkafka.h>

namespace surfingdb {
		namespace connector {

				volatile sig_atomic_t run = 1;

/**
 * @brief Signal termination of program
 */
				void stop(int sig) {
					run = 0;
				}

/**
 * @returns 1 if all bytes are printable, else 0.
 */
				int is_printable(const char *buf, size_t size) {
					size_t i;

					for (i = 0; i < size; i++)
						if (!isprint((int) buf[i]))
							return 0;

					return 1;
				}

				void KafkaConnector::init(std::string topic, std::string brokers) {
					this->brokers = (char *) brokers.c_str();
					std::string groupid = "surfingdb.test";
					this->groupid = (char *) groupid.c_str();
					topics = (char *) topic.c_str();
					topic_cnt = 1;
					conf = rd_kafka_conf_new();

					/* Set bootstrap broker(s) as a comma-separated list of
							 * host or host:port (default port 9092).
							 * librdkafka will use the bootstrap brokers to acquire the full
							 * set of brokers from the cluster. */
					if (rd_kafka_conf_set(conf, "bootstrap.servers", this->brokers,
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
						fprintf(stderr,
						        "%% Failed to subscribe to %d topics: %s\n",
						        subscription->cnt, rd_kafka_err2str(err));
						rd_kafka_topic_partition_list_destroy(subscription);
						rd_kafka_destroy(rk);
					}

					fprintf(stderr,
					        "%% Subscribed to %d topic(s), "
					        "waiting for rebalance and messages...\n",
					        subscription->cnt);

					rd_kafka_topic_partition_list_destroy(subscription);
					signal(SIGINT, stop);
				}

				KafkaConnector::~KafkaConnector() {
					/* Close the consumer: commit final offsets and leave the group. */
					fprintf(stderr, "%% Closing consumer\n");
					rd_kafka_consumer_close(rk);

					/* Destroy the consumer */
					rd_kafka_destroy(rk);
				}

				std::vector<std::shared_ptr<RowBuffer>> KafkaConnector::consume_batch(size_t max_batch_size, int timeout,
				                                                     std::shared_ptr<surfingdb::meta::TableSchema> schema_ptr) {
					auto results = std::vector<std::shared_ptr<RowBuffer>>();
					auto start = MPI_Wtime();
					while ((MPI_Wtime() - start) * 1000 < timeout && results.size() < max_batch_size) {
						rd_kafka_message_t *rkm = rd_kafka_consumer_poll(rk, 10);
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
							fprintf(stderr,
							        "%% Consumer error: %s\n",
							        rd_kafka_message_errstr(rkm));
							rd_kafka_message_destroy(rkm);
							continue;
						}

						/* Proper message. */
						//printf("Message on %s [%" PRId32 "] at offset %" PRId64 ":\n",
						//       rd_kafka_topic_name(rkm->rkt), rkm->partition,
						//       rkm->offset);

						/* Print the message key. */
						//if (rkm->key && is_printable((const char*)rkm->key, rkm->key_len))
						//printf(" Key: %.*s\n",
						//       (int)rkm->key_len, (const char*)rkm->key);
						//else if (rkm->key)
						//printf(" Key: (%d bytes)\n", (int)rkm->key_len);

						/* Print the message value/payload. */
						//if (rkm->payload && is_printable((const char*)rkm->payload, rkm->len))
						//printf(" Value: %.*s\n",
						//       (int)rkm->len, (const char*)rkm->payload);
						//else if (rkm->payload)
						//printf(" Value: (%d bytes)\n", (int)rkm->len);
						auto r = std::make_shared<RowBuffer>(schema_ptr);
						results.push_back(std::move(r));
						rd_kafka_message_destroy(rkm);
					}
					return results;
				}
		}
} // namespace surfingdb