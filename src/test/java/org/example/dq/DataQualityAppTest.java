package org.example.dq;


import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.state.KeyValueStore;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.apache.kafka.common.utils.Utils.*;
import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * End-to-end integration test for {@link DataQualityApp}. Demonstrates
 * how you can programmatically query the REST API exposed by {@link DataQualityRestService}
 */
public class DataQualityAppTest {
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;
    private KeyValueStore<String, String> countStore;

    private final Properties config = mkProperties(mkMap(
            mkEntry(StreamsConfig.APPLICATION_ID_CONFIG, "TestTopicsTest"),
            mkEntry(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")
            //how to add default serdes
    ));

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        String[] columnNames= {"Customer_Id", "First_Name","Last_Name"};

        DataQualityApp.createDQColumnProfilerStateStoreStream(builder,columnNames);
        final Topology topology = builder.build();
        testDriver = new TopologyTestDriver(topology, /*config*/ DataQualityApp.getStreamsConfig());
        inputTopic = testDriver.createInputTopic(DataQualityApp.DEFAULT_INPUT_TOPIC, Serdes.String().serializer(), Serdes.String().serializer());
        outputTopic = testDriver.createOutputTopic(DataQualityApp.DEFAULT_OUTPUT_TOPIC, Serdes.String().deserializer(), Serdes.String().deserializer());
        countStore = testDriver.getKeyValueStore(DataQualityApp.DEFAULT_DQ_STORE);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch(final RuntimeException re) {
            System.out.println(re.getLocalizedMessage());
        }
    }

    @Test
    public void testOneRecord() {
        assertThat(outputTopic.isEmpty(), is(true));
        inputTopic.pipeInput("ID123!");
        assertThat(outputTopic.isEmpty(), is(false));

        KeyValue<String, String> keyValue = outputTopic.readKeyValue();
        assertThat(keyValue, equalTo(new KeyValue<>("Customer_Id", "0:1")));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testMultipleRecords() {
        assertThat(outputTopic.isEmpty(), is(true));
        final List<String> inputValues = Arrays.asList(
                "Apache,Streams,Kafka",
                "Stream,Spark,",
                ",HDFS,Hadoop"
        );
        inputTopic.pipeValueList(inputValues);
        assertThat(outputTopic.isEmpty(), is(false));

        final List<KeyValue<String, String>> expectedColumnMetrics = new ArrayList<>();
        expectedColumnMetrics.add(new KeyValue<String, String>( "Customer_Id", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Customer_Id", "0:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "1:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Customer_Id", "1:3"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:3"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "1:3"));

        final List<KeyValue<String, String>> actualWordCounts = outputTopic.readKeyValuesToList();
        assertThat(actualWordCounts, equalTo(expectedColumnMetrics));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testMultipleRecordsWithMoreColumns() {
        assertThat(outputTopic.isEmpty(), is(true));
        final List<String> inputValues = Arrays.asList(
                "Apache,Streams,Kafka,One More",
                "Stream,Spark,",
                ",HDFS,Hadoop"
        );
        inputTopic.pipeValueList(inputValues);
        assertThat(outputTopic.isEmpty(), is(false));

        final List<KeyValue<String, String>> expectedColumnMetrics = new ArrayList<>();
        expectedColumnMetrics.add(new KeyValue<String, String>( "Customer_Id", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Column4", "0:1"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Customer_Id", "0:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "1:2"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Customer_Id", "1:3"));
        expectedColumnMetrics.add(new KeyValue<String, String>("First_Name", "0:3"));
        expectedColumnMetrics.add(new KeyValue<String, String>("Last_Name", "1:3"));

        final List<KeyValue<String, String>> actualWordCounts = outputTopic.readKeyValuesToList();
        assertThat(actualWordCounts, equalTo(expectedColumnMetrics));

        //No more output in topic
        assertThat(outputTopic.isEmpty(), is(true));
    }

    @Test
    public void testStateStoreMultipleRecords() {
        assertThat(outputTopic.isEmpty(), is(true));
        final List<String> inputValues = Arrays.asList(
                "Apache,Streams,Kafka",
                "Stream,Spark,",
                ",HDFS,"
        );
        inputTopic.pipeValueList(inputValues);
        assertThat(outputTopic.isEmpty(), is(false));

        assertThat(countStore.get("Customer_Id") , equalTo( "1:3"));
        assertThat(countStore.get("First_Name") , equalTo( "0:3"));
        assertThat(countStore.get("Last_Name") , equalTo( "2:3"));
    }

    @Test
    public void testCreateColumnMetricsRecord() {
        String inputData="12, John,Dere, john.dere@example.com,Peachtree, Atlanta,GA,US,100";
        String[] columnNames=
                {"Customer_Id", "First_Name", "Last_Name", "Email", "Street",
                        "City", "State","Country","Order Amount"};
        List<KeyValue<String,String>> result=
                DataQualityApp.createColumnMetricsRecord(inputData,columnNames);
        assertThat(result.isEmpty(),is(false));

        final List<KeyValue<String, String>> expectedColumnNames = new ArrayList<>();
        expectedColumnNames.add(new KeyValue<String, String>( "Customer_Id", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("First_Name", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("Last_Name", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("Email", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("Street", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("City", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("State", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("Country", "0:1"));
        expectedColumnNames.add(new KeyValue<String, String>("Order Amount", "0:1"));
    }

}
