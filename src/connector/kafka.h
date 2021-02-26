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

#ifndef SURFINGDB_KAFKA_H
#define SURFINGDB_KAFKA_H

#include <librdkafka/rdkafkacpp.h>
#include "meta/schema.h"

namespace surfingdb {
		namespace connector {
				struct Segment {
						explicit Segment(int32_t p, int64_t o, size_t s)
										: partition{p}, offset{o}, size{s} {}

						int32_t partition;
						int64_t offset;
						size_t size;

						// build ID as well as a way to serialize the object
						std::string id() const noexcept {
							// this is unique for given topic
							return std::to_string(partition) + "_" + std::to_string(offset) + "_" + std::to_string(size);
						}

						static Segment from(const std::string &id) {
							size_t pos = 0;
							auto ch = id.c_str();

							// look for the first as integer
							size_t delta = 0;
							int32_t p = std::stoi(ch + delta, &pos);
							delta = pos + 1;
							int64_t o = std::stol(ch + delta, &pos);
							delta += pos + 1;
							size_t s = std::stoul(ch + delta, &pos);
							return Segment{p, o, s};
						}
				};

				class KafkaTopic {
				public:
						KafkaTopic(const std::string &brokers,
						           const std::string &topic,
										//const nebula::meta::KafkaSerde &serde,
										   size_t timeoutMs = 5000)
										: brokers_{brokers},
										  topic_{topic},
										//serde_{serde},
										  timeoutMs_{timeoutMs},
										  conf_{nullptr},
										  tconf_{nullptr} {
							init();
						}

						virtual ~KafkaTopic() = default;

				public:
						std::list<Segment> topicByTimestamp(size_t, size_t) noexcept;

						// raw pointer for reference only
						inline RdKafka::Conf *conf() const {
							return conf_.get();
						}

						inline const std::string &topic() const {
							return topic_;
						}

						inline size_t timeoutMs() const {
							return timeoutMs_;
						}

				private:
						bool init() noexcept;

				private:
						std::string brokers_;
						std::string topic_;
						//nebula::meta::KafkaSerde serde_;
						size_t timeoutMs_;

						std::unique_ptr<RdKafka::Conf> conf_;
						std::unique_ptr<RdKafka::Conf> tconf_;
				};

				class KafkaConsumer {
				private:
						std::unique_ptr<RdKafka::Message> msg_;
						size_t timeoutMs_;
						Segment segment_;
						std::unique_ptr<RdKafka::TopicPartition> partition_;
						std::unique_ptr<RdKafka::KafkaConsumer> consumer_;
						static std::unique_ptr<RdKafka::KafkaConsumer> getConsumer(
										const std::string &, const std::unordered_map<std::string, std::string> &);
				public:
						std::unique_ptr<RdKafka::Message> message();
						KafkaConsumer(std::string &topic, std::string& brokers, const std::unordered_map<std::string, std::string> &settings,
						              Segment &segment);
				};


		}
} // namespace surfingdb

#endif //SURFINGDB_KAFKA_H
