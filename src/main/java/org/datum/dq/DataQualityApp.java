/*
 * Copyright the-datum
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.datum.dq;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.state.HostInfo;
import org.apache.kafka.streams.state.KeyValueStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;


public class DataQualityApp {
    private static final String DEFAULT_APPLICATION_ID = "data-quality-app";

    public static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String DEFAULT_RESTSERVICE_HOSTNAME = "localhost";
    public static final int DEFAULT_RESTSERVICE_PORT=1234;
    public static final String DEFAULT_INPUT_TOPIC="streams-plaintext-input";
    public static final String DEFAULT_OUTPUT_TOPIC="streams-data-quality-output";
    public static final String DEFAULT_DQ_STORE="data-quality-store";

    private static String bootstrapServers=DEFAULT_BOOTSTRAP_SERVERS;
    private static String restserviceHostname=DEFAULT_RESTSERVICE_HOSTNAME;
    private static int restservicePort=DEFAULT_RESTSERVICE_PORT;
    private static String inputTopic=DEFAULT_INPUT_TOPIC;
    private static String outputTopic=DEFAULT_OUTPUT_TOPIC;
    private static String dqStore=DEFAULT_DQ_STORE;
    private static String applicationId=DEFAULT_APPLICATION_ID;
    private static String columnNames;
    private static String arrayColumnNames[];

    private static final Logger log = LoggerFactory.getLogger(DataQualityApp.class);

    public static void main(String... args) throws Exception {
        if (args.length > 8) {
            throw new IllegalArgumentException("usage: ... " +
                    "[<input.topic>] (optional, default " + DEFAULT_INPUT_TOPIC + ")] " +
                    "[<output.topic>] (optional, default " + DEFAULT_OUTPUT_TOPIC + ")] " +
                    "[<dq.store>] (optional, default " + DEFAULT_DQ_STORE + ")] " +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] " +
                    "[<restservice.hostname> (optional, default: " + DEFAULT_RESTSERVICE_HOSTNAME + ")]"+
                    "[<restservice.port> (optional, default: " + DEFAULT_RESTSERVICE_PORT + ")]" +
                    "[<application.id> (optional, default: " + DEFAULT_APPLICATION_ID + ")]"+
                    "[<column.names> (optional comma separated string, default:Column1, Column2.....)]"
                    );
        }


        inputTopic = args.length==1?args[0].trim():DEFAULT_INPUT_TOPIC;
        outputTopic = args.length > 1?args[1].trim():DEFAULT_OUTPUT_TOPIC;
        dqStore = args.length > 2?args[2].trim():DEFAULT_DQ_STORE;
        bootstrapServers = args.length > 3 ? args[3] : DEFAULT_BOOTSTRAP_SERVERS;
        restserviceHostname = args.length > 4 ? args[4] : DEFAULT_RESTSERVICE_HOSTNAME;
        restservicePort = args.length > 5 ? Integer.parseInt(args[5]) : DEFAULT_RESTSERVICE_PORT;
        applicationId = args.length > 6 ? args[6]:DEFAULT_APPLICATION_ID;
        columnNames=args.length > 7?args[7]:"";
        arrayColumnNames=columnNames.trim().split(",");

        log.info("Connecting to Kafka cluster via bootstrap servers " + bootstrapServers);
        final Properties props = getStreamsConfig();
        final StreamsBuilder builder = new StreamsBuilder();
        createDQColumnProfilerStateStoreStream(builder,arrayColumnNames);

        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);

        log.info("Starting REST service endpoint at http://" + restserviceHostname + ":" + restservicePort);
        final DataQualityRestService restService =
                startRestService(streams, restserviceHostname, restservicePort);


        final CountDownLatch latch = new CountDownLatch(1);
        // attach shutdown handler to catch control-c
        // Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                restService.stop();
                streams.close();
            } catch (final Exception e) {
                // ignored
            }
        }));

        try {
            System.out.println("Press CTRL+C to stop");
            streams.cleanUp();
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);


    }

    static List<KeyValue<String,String>> createColumnMetricsRecord(String inputData,
                        final String[] arrColumnNames) {
        List<KeyValue<String, String>> result = new LinkedList<>();
        //simple way to split CSV Record, need a robust solution
        //-1 to include empty column at the end of the record
        String[]  records = inputData.split(",",-1);
        for(int i=0;i<records.length;i++) {
            //determine column name for key
            String columnName="Column" + (i+1);
            if(i<arrColumnNames.length) {
                columnName = arrColumnNames[i];
            }
            result.add(KeyValue.pair(columnName,
                    (records[i].isEmpty()?"1":"0") + ":" + "1"));
        }
        return result;
    }

    static void createDQColumnProfilerStateStoreStream(final StreamsBuilder builder, final String[] columnNames) {
        KStream<String, String> textLines = builder.stream(inputTopic);
        KStream<String, String> columnMetrics = textLines
                .flatMap((key, textLineValue) ->
                    createColumnMetricsRecord(textLineValue,columnNames));

        KTable<String,String> columnMetricsSummary=columnMetrics.groupByKey()
                    .reduce((aggValue,newValue)-> {
                        String[] aggValues=aggValue.split(":");
                        String[] newValues = newValue.split(":");
                        int empty=Integer.valueOf(aggValues[0]) + Integer.valueOf(newValues[0]);
                        int count=Integer.valueOf(aggValues[1]) + Integer.valueOf(newValues[1]);
                        return empty +":"+count;
                    },
                    Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as(dqStore)
                    );

        columnMetricsSummary.toStream().to(outputTopic,Produced.with(Serdes.String(), Serdes.String()));
    }


    static Properties getStreamsConfig() /*throws IOException*/ {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        // *IMP* Provide the details of embedded http service to connect to the streams instance and discover locations of stores.
        props.put(StreamsConfig.APPLICATION_SERVER_CONFIG,restserviceHostname + ":" + restservicePort);
        //final File example = Files.createTempDirectory(new File("/tmp").toPath(), "example").toFile();
        //props.put(StreamsConfig.STATE_DIR_CONFIG, example.getPath());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    static DataQualityRestService startRestService(KafkaStreams streams, final String host, final int port)
            throws Exception {

        final HostInfo hostInfo = new HostInfo(host, port);
        final DataQualityRestService dataQualityRestService = new DataQualityRestService(streams, hostInfo);
        dataQualityRestService.start(port);
        return dataQualityRestService;
    }
}
