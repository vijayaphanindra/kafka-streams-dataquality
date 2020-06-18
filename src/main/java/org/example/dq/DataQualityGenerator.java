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
package org.example.dq;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

public class DataQualityGenerator {
    static final String DEFAULT_INPUT_TOPIC = "streams-plaintext-input";
    private static final String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";

    private static final Logger log = LoggerFactory.getLogger(DataQualityGenerator.class);

    private static String bootstrapServers;
    private static String inputTopic;

    public static void main(String... args) throws Exception {
        log.info("Input Arguments: " + args.toString());
        if (args.length > 2) {
            throw new IllegalArgumentException("usage: ... " +
                    "[<topic.name>] (optional, default: " + DEFAULT_INPUT_TOPIC +
                    "[<bootstrap.servers> (optional, default: " + DEFAULT_BOOTSTRAP_SERVERS + ")] ");
        }

        inputTopic = args.length==1?args[0].trim():DEFAULT_INPUT_TOPIC;
        bootstrapServers = args.length > 1 ? args[1] : DEFAULT_BOOTSTRAP_SERVERS;


        final List<String> inputValues = Arrays.asList(
                "12, John,Dere, john.dere@example.com,Peachtree, Atlanta,GA,US,100",
                "47, Tom,,,Bay Area, Atlanta,GA,US,150",
                "323, Bala,Ananda, bala_ananada@acme.com,Sadandanagar, Bengaluru,Karnataka,India,20",
                "51, Rita, Lindin, ritalindin@example.com,Pacific Ave, Dallas,TX,US,34",
                "1234, Ashok,B, ,, Mountain View,CA,US,11",
                "921, Greg,George, ,Olive Ave,Sunny Vale,CA,US,23",
                "123, Rama,K, ,Near Temple,Golkonda,Hyderabad,India,111",
                "11, Bill,Robert, bill.rober@example.com,, Redmond,WA,US,76",
                "1001, Andy,George, ,, New York,NY,US,23"
        );

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, Serdes.String().getClass());

        final KafkaProducer<String, String> producer =
                new KafkaProducer<String, String>(props, new StringSerializer(), new StringSerializer());

        producer.send(new ProducerRecord<>(inputTopic,
                inputValues.get(0), inputValues.get(0)));

        final Random random = new Random();
        while (true) {
            final int i = random.nextInt(inputValues.size());
            producer.send(new ProducerRecord<>(inputTopic, inputValues.get(i), inputValues.get(i)));
            Thread.sleep(2000L);
        }

    }
}
