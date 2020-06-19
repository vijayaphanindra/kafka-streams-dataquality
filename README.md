# Kafka Streams Data Quality Tools
The Kafka Streams Dataquality application demonstrate how to use Kafka Streams API and interactive queries to compute data quality metrics in realtime and how the metrics state store can be exposed via REST API.

For more information refer to detailed blog post [Streaming data quality — How to implement column profiling using Kafka Streams?— Data Quality Series](https://medium.com/@vijay_b/streaming-data-quality-how-to-implement-column-profiling-using-kafka-streams-4a3b2f1245ca)

---

```shell
# Start zookeeper first and end last
bin/zookeeper-server-start.sh config/zookeeper.properties

# Start Kafka after zookeer and Stop before zookeer
bin/kafka-server-start.sh config/server.properties

# Create a sample topic
bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic streams-plaintext-input

# Run the data generator application from jar build with this repo
$ java -cp target/kafka-streams-dataquality-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
        org.datum.dq.DataQualityGenerator
 
# Run the data generator application from jar build with this repo
$ java -cp target/kafka-streams-dataquality-1.0-SNAPSHOT-jar-with-dependencies.jar \ 
        org.datum.dq.DataQualityApp \
        "streams-plaintext-input" "streams-data-quality-output" \
        "data-quality-store" "localhost:9092" "localhost" 1234 \ 
        "data-quality-app" "Customer_Id,First_Name,Last_Name,Email,Street"
```

Query the Rest Service
```shell script
curl -sXGET  http://localhost:1234/dq/instances
```

```json
[
   {
      "host":"localhost",
      "port":1234,
      "storeNames":[
         "data-quality-store"
      ]
   }
]
```
```shell script
curl -sXGET http://localhost:1234/dq/cp/data-quality-store
```

```json
[
   {
      "column":"Customer_Id",
      "emptyCount":0,
      "count":86,
      "completeness":1.0
   },
   {
      "column":"Email",
      "emptyCount":9,
      "count":86,
      "completeness":0.89534885
   },
   {
      "column":"First_Name",
      "emptyCount":0,
      "count":86,
      "completeness":1.0
   },
   {
      "column":"Last_Name",
      "emptyCount":9,
      "count":86,
      "completeness":0.89534885
   },
   {
      "column":"Street",
      "emptyCount":31,
      "count":86,
      "completeness":0.6395349
   }
]
```
