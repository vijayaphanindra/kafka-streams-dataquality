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

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.*;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.datum.dq.pojo.ColumnMetrics;
import org.glassfish.jersey.jackson.JacksonFeature;
import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.core.MediaType;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Path("dq")
public class DataQualityRestService {

    private final KafkaStreams streams;
    //private final Collection<StreamsMetadata> metadata;
    private Server jettyServer;
    private final HostInfo hostInfo;
    private final Client client = ClientBuilder.newBuilder().register(JacksonFeature.class).build();
    private static final Logger log = LoggerFactory.getLogger(DataQualityRestService.class);

    public DataQualityRestService(KafkaStreams streams, HostInfo hostInfo) {
        this.hostInfo = hostInfo;
        this.streams = streams;
    }

    /**
     * Get the metadata for all of the instances of this Kafka Streams application
     *
     * @return List of {@link HostStoreInfo}
     */
    @GET()
    @Path("/instances")
    @Produces(MediaType.APPLICATION_JSON)
    public List<HostStoreInfo> streamsMetadata() {
        log.info("Received request");
        final Collection<StreamsMetadata> metadata = streams.allMetadata();
        return mapInstancesToHostStoreInfo(metadata);
    }

     // Get all of the key-value pairs available in a store

    /**
     * Get all of the key-value pairs available in a store
     * @param storeName
     * @return List of {@link ColumnMetrics}
     */
    @GET()
    @Path("/cp/{storeName}")
    @Produces(MediaType.APPLICATION_JSON)
    public List<ColumnMetrics> allForStore(@PathParam("storeName") final String storeName) {
        return rangeForKeyValueStore(storeName, ReadOnlyKeyValueStore::all);
    }


    /**
     * Performs a range query on a KeyValue Store and converts the results into a List of ColumnMetrics
     * @param storeName
     * @param rangeFunction
     * @return List of {@link ColumnMetrics}
     */
    private List<ColumnMetrics> rangeForKeyValueStore(final String storeName,
                        final Function<ReadOnlyKeyValueStore<String, String>,
                            KeyValueIterator<String, String>> rangeFunction) {

        // Get the KeyValue Store
        final ReadOnlyKeyValueStore<String, String> store =
                streams.store(storeName, QueryableStoreTypes.keyValueStore());
        final List<ColumnMetrics> results = new ArrayList<>();
        // Apply the function, i.e., query the store
        final KeyValueIterator<String, String> range = rangeFunction.apply(store);

        // Convert the results
        while (range.hasNext()) {
            final KeyValue<String, String> next = range.next();
            String[] values = next.value.split(":");
            int emptyCount = Integer.valueOf(values[0]);
            int count=Integer.valueOf(values[1]);
            float completeness = (float)(count-emptyCount)/count;
            results.add(new ColumnMetrics(next.key,emptyCount,count,
                    completeness));
        }
        return results;
    }


    /**
     * Start an embedded Jetty Server on the given port
     *
     * @param port port to run the Server on
     * @throws Exception if jetty can't start
     */
    void start(final int port) throws Exception {
        final ServletContextHandler context = new ServletContextHandler(ServletContextHandler.SESSIONS);
        context.setContextPath("/");

        jettyServer = new Server();
        jettyServer.setHandler(context);

        final ResourceConfig rc = new ResourceConfig();
        rc.register(this);
        rc.register(JacksonFeature.class);

        final ServletContainer sc = new ServletContainer(rc);
        final ServletHolder holder = new ServletHolder(sc);
        context.addServlet(holder, "/*");

        final ServerConnector connector = new ServerConnector(jettyServer);
        connector.setHost(hostInfo.host());
        connector.setPort(port);
        jettyServer.addConnector(connector);

        context.start();

        try {
            jettyServer.start();
        } catch (final java.net.SocketException exception) {
            log.error("Unavailable: " + hostInfo.host() + ":" + hostInfo.port());
            throw new Exception(exception.toString());
        }
    }

    /**
     * Stop the Jetty Server
     *
     * @throws Exception if jetty can't stop
     */
    void stop() throws Exception {
        if (jettyServer != null) {
            jettyServer.stop();
        }
    }

    private List<HostStoreInfo> mapInstancesToHostStoreInfo(
            final Collection<StreamsMetadata> metadatas) {
        return metadatas.stream().map(metadata -> new HostStoreInfo(metadata.host(),
                metadata.port(),
                metadata.stateStoreNames()))
                .collect(Collectors.toList());
    }
}
