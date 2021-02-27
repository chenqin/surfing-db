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
#include <iostream>
#include <mpi.h>
#include <sys/time.h>
#include <csignal>

namespace surfingdb {
		namespace connector {
				static volatile sig_atomic_t run = 1;
				static bool exit_eof = false;
				static int eof_cnt = 0;
				static int partition_cnt = 0;
				static int verbosity = 1;
				static long msg_cnt = 0;
				static int64_t msg_bytes = 0;

				static void sigterm(int sig) {
					run = 0;
				}

				class ExampleEventCb : public RdKafka::EventCb {
				public:
						void event_cb(RdKafka::Event &event) {
							switch (event.type()) {
								case RdKafka::Event::EVENT_ERROR:
									if (event.fatal()) {
										std::cerr << "FATAL ";
										run = 0;
									}
									std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
									          event.str() << std::endl;
									break;

								case RdKafka::Event::EVENT_STATS:
									std::cerr << "\"STATS\": " << event.str() << std::endl;
									break;

								case RdKafka::Event::EVENT_LOG:
									fprintf(stderr, "LOG-%i-%s: %s\n",
									        event.severity(), event.fac().c_str(), event.str().c_str());
									break;

								case RdKafka::Event::EVENT_THROTTLE:
									std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
									          event.broker_name() << " id " << (int) event.broker_id() << std::endl;
									break;

								default:
									std::cerr << "EVENT " << event.type() <<
									          " (" << RdKafka::err2str(event.err()) << "): " <<
									          event.str() << std::endl;
									break;
							}
						}
				};

				class ExampleRebalanceCb : public RdKafka::RebalanceCb {
				private:
						static void part_list_print(const std::vector<RdKafka::TopicPartition *> &partitions) {
							for (unsigned int i = 0; i < partitions.size(); i++)
								std::cerr << partitions[i]->topic() <<
								          "[" << partitions[i]->partition() << "], ";
							std::cerr << "\n";
						}

				public:
						void rebalance_cb(RdKafka::KafkaConsumer *consumer,
						                  RdKafka::ErrorCode err,
						                  std::vector<RdKafka::TopicPartition *> &partitions) {
							std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

							part_list_print(partitions);

							RdKafka::Error *error = NULL;
							RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

							if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
								if (consumer->rebalance_protocol() == "COOPERATIVE")
									error = consumer->incremental_assign(partitions);
								else
									ret_err = consumer->assign(partitions);
								partition_cnt += (int) partitions.size();
							} else {
								if (consumer->rebalance_protocol() == "COOPERATIVE") {
									error = consumer->incremental_unassign(partitions);
									partition_cnt -= (int) partitions.size();
								} else {
									ret_err = consumer->unassign();
									partition_cnt = 0;
								}
							}
							eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

							if (error) {
								std::cerr << "incremental assign failed: " << error->str() << "\n";
								delete error;
							} else if (ret_err)
								std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";

						}
				};

				class ExampleRebalanceCb : public RdKafka::RebalanceCb {
				private:
						static void part_list_print(const std::vector<RdKafka::TopicPartition *> &partitions) {
							for (unsigned int i = 0; i < partitions.size(); i++)
								std::cerr << partitions[i]->topic() <<
								          "[" << partitions[i]->partition() << "], ";
							std::cerr << "\n";
						}

				public:
						void rebalance_cb(RdKafka::KafkaConsumer *consumer,
						                  RdKafka::ErrorCode err,
						                  std::vector<RdKafka::TopicPartition *> &partitions) {
							std::cerr << "RebalanceCb: " << RdKafka::err2str(err) << ": ";

							part_list_print(partitions);

							RdKafka::Error *error = NULL;
							RdKafka::ErrorCode ret_err = RdKafka::ERR_NO_ERROR;

							if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
								if (consumer->rebalance_protocol() == "COOPERATIVE")
									error = consumer->incremental_assign(partitions);
								else
									ret_err = consumer->assign(partitions);
								partition_cnt += (int) partitions.size();
							} else {
								if (consumer->rebalance_protocol() == "COOPERATIVE") {
									error = consumer->incremental_unassign(partitions);
									partition_cnt -= (int) partitions.size();
								} else {
									ret_err = consumer->unassign();
									partition_cnt = 0;
								}
							}
							eof_cnt = 0; /* FIXME: Won't work with COOPERATIVE */

							if (error) {
								std::cerr << "incremental assign failed: " << error->str() << "\n";
								delete error;
							} else if (ret_err)
								std::cerr << "assign failed: " << RdKafka::err2str(ret_err) << "\n";

						}
				};

				static int64_t now() {
#ifndef _WIN32
					struct timeval tv;
					gettimeofday(&tv, NULL);
					return ((int64_t) tv.tv_sec * 1000) + (tv.tv_usec / 1000);
#else
#error "now() not implemented for Windows, please submit a PR"
#endif
				}

				KafkaConnector::KafkaConnector(std::vector<std::string> &topics, std::string &brokers) {
					std::string errstr;
					conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
					//RdKafka::Conf *tconf = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
					ExampleRebalanceCb ex_rebalance_cb;
					if (conf->set("rebalance_cb", &ex_rebalance_cb, errstr) != RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}
					ExampleEventCb ex_event_cb;
					if (conf->set("event_cb", &ex_event_cb, errstr) != RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}

					if (conf->set("enable.partition.eof", "false", errstr) != RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}
					if (conf->set("group.id", "surfingdb.test", errstr) != RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}
					if (conf->set("bootstrap.servers", brokers, errstr) != RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}

					if (conf->set("compression.codec", codec, errstr) !=
					    RdKafka::Conf::CONF_OK) {
						std::cerr << errstr << std::endl;
					}

					//conf->set("default_topic_conf", tconf, errstr);
					//delete tconf;

					RdKafka::KafkaConsumer::create(conf, errstr);
					if (!consumer) {
						std::cerr << "Failed to create consumer: " << errstr << std::endl;
					}
					delete conf;
					/* Subscribe to topics */
					RdKafka::ErrorCode err = consumer->subscribe(topics);
					if (err) {
						std::cerr << "Failed to subscribe to " << topics.size() << " topics: "
						          << RdKafka::err2str(err) << std::endl;

					}
				}

				std::vector<RdKafka::Message *>
				KafkaConnector::consume_batch(size_t batch_size, int batch_tmout) {

					std::vector<RdKafka::Message *> msgs;
					msgs.reserve(batch_size);

					int64_t end = now() + batch_tmout;
					int remaining_timeout = batch_tmout;

					while (msgs.size() < batch_size) {
						RdKafka::Message *msg = consumer->consume(remaining_timeout);

						switch (msg->err()) {
							case RdKafka::ERR__TIMED_OUT:
								delete msg;
								return msgs;

							case RdKafka::ERR_NO_ERROR:
								msgs.push_back(msg);
								break;

							default:
								std::cerr << "%% Consumer error: " << msg->errstr() << std::endl;
								delete msg;
								return msgs;
						}

						remaining_timeout = end - now();
						if (remaining_timeout < 0)
							break;
					}

					return msgs;
				}

				void KafkaConnector::msg_consume(RdKafka::Message *message, void *opaque) {
					switch (message->err()) {
						case RdKafka::ERR__TIMED_OUT:
							break;

						case RdKafka::ERR_NO_ERROR:
							/* Real message */
							msg_cnt++;
							msg_bytes += message->len();
							if (verbosity >= 3)
								std::cerr << "Read msg at offset " << message->offset() << std::endl;
							RdKafka::MessageTimestamp ts;
							ts = message->timestamp();
							if (verbosity >= 2 &&
							    ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE) {
								std::string tsname = "?";
								if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
									tsname = "create time";
								else if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
									tsname = "log append time";
								std::cout << "Timestamp: " << tsname << " " << ts.timestamp << std::endl;
							}
							if (verbosity >= 2 && message->key()) {
								std::cout << "Key: " << *message->key() << std::endl;
							}
							if (verbosity >= 1) {
								printf("%.*s\n",
								       static_cast<int>(message->len()),
								       static_cast<const char *>(message->payload()));
							}
							break;

						case RdKafka::ERR__PARTITION_EOF:
							/* Last message */
							if (exit_eof && ++eof_cnt == partition_cnt) {
								std::cerr << "%% EOF reached for all " << partition_cnt <<
								          " partition(s)" << std::endl;
								run = 0;
							}
							break;

						case RdKafka::ERR__UNKNOWN_TOPIC:
						case RdKafka::ERR__UNKNOWN_PARTITION:
							std::cerr << "Consume failed: " << message->errstr() << std::endl;
							run = 0;
							break;

						default:
							/* Errors */
							std::cerr << "Consume failed: " << message->errstr() << std::endl;
							run = 0;
					}
				}

				KafkaConnector::~KafkaConnector() {
					consumer->unsubscribe();
				}
		}
} // namespace surfingdb