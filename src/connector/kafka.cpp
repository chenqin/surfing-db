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
#include <librdkafka/rdkafkacpp.h>

namespace surfingdb {
		namespace connector {
				class KafkaEventCb : public RdKafka::EventCb {
				public:
						virtual void event_cb(RdKafka::Event &event) override {
							switch (event.type()) {
								case RdKafka::Event::EVENT_ERROR:
									if (event.fatal()) {
										LOG(FATAL) << "event error";
									}
									LOG(ERROR) << "ERROR (" << RdKafka::err2str(event.err()) << "): " << event.str();
									break;

								case RdKafka::Event::EVENT_STATS:
									// LOG(ERROR) << "\"STATS\": " << event.str();
									break;

								case RdKafka::Event::EVENT_LOG:
									LOG(ERROR) << event.severity() << "-" << event.fac().c_str() << "-" << event.str().c_str();
									break;

								case RdKafka::Event::EVENT_THROTTLE:
									LOG(ERROR) << "THROTTLED: " << event.throttle_time() << "ms by " << event.broker_name() << " id "
									           << (int) event.broker_id();
									break;

								default:
									LOG(ERROR) << "EVENT " << event.type() << " (" << RdKafka::err2str(event.err()) << "): "
									           << event.str();
									break;
							}
						}
				};

				class SegmentedRebalanceCb : public RdKafka::RebalanceCb {
				public:
						SegmentedRebalanceCb(Segment segment) : segment_{std::move(segment)} {
						}

				public:
						virtual void rebalance_cb(RdKafka::KafkaConsumer *consumer,
						                          RdKafka::ErrorCode err,
						                          std::vector<RdKafka::TopicPartition *> &partitions) override {

							// pick the partition we want to target
							RdKafka::TopicPartition *target = nullptr;
							for (auto p : partitions) {
								if (p->partition() == segment_.partition) {
									target = p;
									break;
								}
							}

							CHECK_NOTNULL(target);

							// set offset of this partition
							target->set_offset(segment_.offset);

							if (err == RdKafka::ERR__ASSIGN_PARTITIONS) {
								consumer->assign({target});
							} else {
								consumer->unassign();
							}
						}

				private:
						Segment segment_;
				};

				bool KafkaTopic::init() noexcept {
					// set up the kafka configurations
					std::string error;
					conf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
					tconf_ = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

#define SET_KEY_VALUE_CHECK(K, V)                            \
  if (conf_->set(#K, #V, error) != RdKafka::Conf::CONF_OK) { \
    LOG(ERROR) << "Kafka: " << error;                        \
    return false;                                            \
  }

					// set brokers
					conf_->set("metadata.broker.list", brokers_, error);

					// group Id is a must
					SET_KEY_VALUE_CHECK(group.id, 0)

					// set default topic config
					conf_->set("default_topic_conf", tconf_.get(), error);

#undef SET_KEY_VALUE_CHECK
					return true;
				}

// based on the start time, return a kafka segment list
// this function will generate segments for each partition given start time stamp and segment width
// it is querying the start and end offset by the time condition of each partition
// and figure out "STARTING point" of each segment by width (W) in the line of
//  [0, W) [W, 2W) [2W, 3W) .... [NW, (N+1)W)
				std::list<Segment> KafkaTopic::topicByTimestamp(size_t timeMs, size_t width) noexcept {
					std::list<Segment> segments;
					std::string error;

					// create a consumer
					auto consumer = std::unique_ptr<RdKafka::KafkaConsumer>(RdKafka::KafkaConsumer::create(conf_.get(), error));
					if (!consumer) {
						LOG(ERROR) << "Kafka: " << error;
						return segments;
					}

					// figure out partition count
					auto topic = std::unique_ptr<RdKafka::Topic>(
									RdKafka::Topic::create(consumer.get(), topic_, tconf_.get(), error));
					if (!topic) {
						LOG(ERROR) << "Kafka: " << error;
						return segments;
					}

					// fetch metadata
					RdKafka::Metadata *metadata;
					if (consumer->metadata(false, topic.get(), &metadata, timeoutMs_) != RdKafka::ERR_NO_ERROR) {
						LOG(ERROR) << "Kafka: can not fetch metadata";
						return segments;
					}

					// save metadata info
					auto topicMetadata = metadata->topics()->at(0);
					auto partitions = topicMetadata->partitions();
					if (partitions->size() == 0) {
						LOG(ERROR) << "Kafka: topic has no partitions.";
						return segments;
					}

					std::vector<int32_t> pids;
					pids.reserve(partitions->size());
					std::transform(partitions->cbegin(), partitions->cend(),
					               std::back_insert_iterator(pids),
					               [](auto p) {
							               return p->id();
					               });

					// delete metadata object and return
					delete metadata;

					// overwrite width if this topic specified size
					//if (serde_.size > 0) {
					//width = serde_.size;
					//}

					for (auto part : pids) {
						// create partition pointing to a specific time stamp
						auto p = std::unique_ptr<RdKafka::TopicPartition>(
										RdKafka::TopicPartition::create(topic_, part, timeMs));
						std::vector<RdKafka::TopicPartition *> ps{p.get()};
						if (consumer->assign(ps) != RdKafka::ERR_NO_ERROR) {
							LOG(ERROR) << "Kafka: partition assignment failed.";

							return segments;
						}

						auto err = consumer->offsetsForTimes(ps, timeoutMs_);
						auto startOffset = p->offset();
						if (err != RdKafka::ERR_NO_ERROR) {
							LOG(ERROR) << "Kafka: failed to call offset for time: " << timeMs << ", err=" << err;
							startOffset = -1;
						}

						// get high end
						int64_t lowOffset = -1;
						int64_t highOffset = -1;
						if (consumer->query_watermark_offsets(
										topic_, part, &lowOffset, &highOffset, timeoutMs_)
						    != RdKafka::ERR_NO_ERROR) {
							LOG(ERROR) << "Kafka: failed to query watermark offsets.";
							continue;
						}

						// if the broker doesn't support offsetsForTimes, we have to assuming
						if (startOffset == -1) {
							// try to use serde retention in seconds
							// to estimate the range of offsets.
							startOffset = lowOffset;
							/*
							if (serde_.retention > 0) {
								auto retentionMs = serde_.retention * 1000.0;
								auto timeRange = (nebula::common::Evidence::unix_timestamp() * 1000 - timeMs);
								startOffset = highOffset - (timeRange / retentionMs) * (highOffset - lowOffset);
								LOG(INFO) << "Estimate start offset by retention: " << serde_.retention
								          << ", start=" << startOffset
								          << ", low=" << lowOffset
								          << ", high=" << highOffset
								          << ", timems=" << timeRange;
							}*/
						}

						// we use this range [startOffset, highOffset] to pick the latest range [start, end)
						// any future updates will cover uncovered ranges
						auto start = startOffset / width;
						auto end = highOffset / width;
						// if no segements to generate, log a warning
						if (start >= end) {
							LOG(WARNING) << "No segments to produce: batch=" << width
							             << ", start-off=" << startOffset << ", low-off=" << lowOffset << ", high-off=" << highOffset
							             << ", partition=" << part << ", topic=" << topic_;
						}

						// Here places an interesting case, because end=(highOffset/width),
						// so any message in current band not full to a batch will not be consumed.
						// for example, width is 1000, the last number of message passing x*1000,
						// let's say 900 won't be consumed until it's filled up
						// This problem potentially will apply a latency between latest message and time when it show up in Nebula.
						while (start < end) {
							segments.emplace_back(part, width * start++, width);
						}

						// unassign
						consumer->unassign();
					}

					// close the consumer
					consumer->close();

					return segments;
				}


				// Kafka consumer handle is expensive resource which is supposed to reuse
// in the same thread.
				std::unique_ptr<RdKafka::KafkaConsumer> KafkaConsumer::getConsumer(
								const std::string &brokers,
								const std::unordered_map<std::string, std::string> &settings) {
					// set up the kafka configurations
					std::string error;
					auto conf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL));
					auto tconf = std::unique_ptr<RdKafka::Conf>(RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC));

#define SET_KEY_VALUE_CHECK(K, V)                         \
  if (conf->set(K, V, error) != RdKafka::Conf::CONF_OK) { \
    LOG(INFO) << "Kafka: " << error;                      \
    return nullptr;                                       \
  }

					// for kafka - the configs are all go to consumers
					constexpr std::string_view KAFKA_PFX = "kafka.";
					constexpr auto KAFKA_PFX_LEN = KAFKA_PFX.size();
					for (auto itr = settings.begin(); itr != settings.end(); ++itr) {
						const auto &key = itr->first;
						LOG(INFO) << key << ":" << itr->second;
						/*
						if (Chars::prefix(key.data(), key.size(), KAFKA_PFX.data(), KAFKA_PFX_LEN)) {
							std::string realKey(key.data() + KAFKA_PFX_LEN, key.size() - KAFKA_PFX_LEN);
							LOG(INFO) << "Set kafka config from user settings: " << realKey;
							SET_KEY_VALUE_CHECK(realKey, itr->second);
						}
						 */
					}

					// set brokers
					SET_KEY_VALUE_CHECK("metadata.broker.list", brokers)

					// set group id anyways even we don't use consumer group at all
					SET_KEY_VALUE_CHECK("group.id", "surfingdb.kafka");

					// const auto INTEGER_MAX = std::to_string(std::numeric_limits<int32_t>::max());
					SET_KEY_VALUE_CHECK("max.poll.interval.ms", "86400000");

					// set event callback by a static instance
					static KafkaEventCb ecb;
					SET_KEY_VALUE_CHECK("event_cb", &ecb)

					// set default topic config
					SET_KEY_VALUE_CHECK("default_topic_conf", tconf.get())

#undef SET_KEY_VALUE_CHECK

					auto ptr = RdKafka::KafkaConsumer::create(conf.get(), error);
					CHECK_NOTNULL(ptr);
					return std::unique_ptr<RdKafka::KafkaConsumer>(ptr);
				}

				// build the subscription pipeline
				KafkaConsumer::KafkaConsumer(std::string &topic, std::string &brokers, const std::unordered_map<std::string, std::string> &settings,
				                             Segment& segment) : segment_(segment) {
					// subscribe is designed for group balance, we use assign directly
					LOG(INFO) << "Consume " << topic << "/" << topic << ":" << segment_.id();
					partition_ = std::unique_ptr<RdKafka::TopicPartition>(
									RdKafka::TopicPartition::create(topic, segment_.partition, timeoutMs_));

					auto offset = segment_.offset;

					// assign the partition to start consuming
					consumer_ = KafkaConsumer::getConsumer(brokers, settings);

					// a segment can have offset even smaller than valid range of the partition
					// due to range chunking, adjust partition offset if this is the case
					int64_t lowOffset = -1;
					int64_t highOffset = -1;
					if (consumer_->query_watermark_offsets(
									topic, segment_.partition, &lowOffset, &highOffset, timeoutMs_)
					    == RdKafka::ERR_NO_ERROR
					    && lowOffset > offset) {
						offset = lowOffset;
						LOG(INFO) << "Adjust partition offset to low bound.";
					}

					// set partition offset to read and assign to current consumer
					partition_->set_offset(offset);
					consumer_->assign({ partition_.get() });
/*
					// create parser
					// support thrift binary and json
					if (table_->format == "thrift" && table_->serde.protocol == "binary") {
						parser_ = std::make_unique<ThriftRow>(table_->serde.cmap);
					} else if (table_->format == "json") {
						parser_ = std::make_unique<JsonRow>(nebula::type::TypeSerializer::from(table_->schema));
					} else {
						throw NException("Only support thrift(TBinaryProtocol) and JSON for now.");
					}
*/
					// set errors to 0 and set maximum messages to load
					auto errors_ = 0;

					// load the first message
					msg_ = message();
				}

				std::unique_ptr<RdKafka::Message> KafkaConsumer::message() {

					// when this message is consumed from queue, please delete it
					std::unique_ptr<RdKafka::Message> msg(consumer_->consume(timeoutMs_));

					// message may be empty
					if (msg && msg->len() > 0) {
						// check if the message has error
						if (msg->err() != RdKafka::ERR_NO_ERROR) {
							LOG(ERROR) << "Error in reading kafka message: " << msg->errstr();

						}
					}
					return msg;
				}

				inline size_t readMessageTimestamp(const RdKafka::Message& msg) {
					// convert to seconds, nebula use seconds as timestamp
					return (size_t)msg.timestamp().timestamp / 1000;
					// if (ts.type != RdKafka::MessageTimestamp::MSG_TIMESTAMP_NOT_AVAILABLE)
					// if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_CREATE_TIME)
					// if (ts.type == RdKafka::MessageTimestamp::MSG_TIMESTAMP_LOG_APPEND_TIME)
				}
		}
} // namespace surfingdb