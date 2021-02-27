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

#include <librdkafka/rdkafka.h>
#include <vector>
#include <string>

namespace surfingdb {
		namespace connector {
				/**
				 * thin kafka client wrapper
				 * https://docs.confluent.io/5.5.1/clients/librdkafka/md_CONFIGURATION.html
				 */
				class KafkaConnector {
				public:
            void init(std::string, std::string);
            KafkaConnector() {}
            std::vector<rd_kafka_message_t *> consume_batch(size_t batch_size, int batch_tmout);
						~KafkaConnector();
				private:
          rd_kafka_t *rk;          /* Consumer instance handle */
          rd_kafka_conf_t *conf;   /* Temporary configuration object */
          rd_kafka_resp_err_t err; /* librdkafka API error code */
          char errstr[512];        /* librdkafka API error reporting buffer */
          const char *brokers;     /* Argument: broker list */
          const char *groupid;     /* Argument: Consumer group id */
          char *topics;           /* Argument: list of topics to subscribe to */
          int topic_cnt;           /* Number of topics to subscribe to */
          rd_kafka_topic_partition_list_t *subscription; /* Subscribed topics */
          int i;
				};
		} // namespace surfingdb

}
#endif //SURFINGDB_KAFKA_H
