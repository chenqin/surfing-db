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

namespace surfingdb {
		namespace connector {
				/**
				 * thin kafka client wrapper
				 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
				 */
				class KafkaConnector {
				public:
						std::vector<RdKafka::Message *>
						consume_batch(size_t batch_size, int batch_tmout);

						KafkaConnector(std::vector<std::string> &topics, std::string &brokers);
						~KafkaConnector();
				private:
						RdKafka::KafkaConsumer *consumer;
						RdKafka::Conf *conf;
						std::string codec = "none";
						void msg_consume(RdKafka::Message *message, void *opaque);
				};
		} // namespace surfingdb

}
#endif //SURFINGDB_KAFKA_H
