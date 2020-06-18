
# Start zookeeper first and end last
bin/zookeeper-server-start.sh config/zookeeper.properties

#Start Kafka after zookeer and Stop before zookeer
bin/kafka-server-start.sh config/server.properties

#create topics
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input

#delete topics
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092  --topic streams-plaintext-input

#list topics
bin/kafka-topics.sh --list --bootstrap-server localhost:9092

#Produce Messages
bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic streams-plaintext-input

#Consume messages
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic treams-data-quality-output --from-beginning

#
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092  --topic data-quality-app-data-quality-store-changelog
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092  --topic data-quality-app-data-quality-store-repartition
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092  --topic streams-data-quality-output
bin/kafka-topics.sh --delete --bootstrap-server localhost:9092  --topic streams-plaintext-input

#command line parameters
"streams-plaintext-input" "streams-data-quality-output" "data-quality-store" "localhost:9092" "localhost" 1234 "data-quality-app" "Customer_Id,First_Name,Last_Name,Email,Street"

#
bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic streams-wordcount-output --from-beginning \
       --property print.key=true \
       --property print.value=true \
       --property key.deserializer=org.apache.kafka.common.serialization.StringDeserializer \
       --property value.deserializer=org.apache.kafka.common.serialization.LongDeserializer