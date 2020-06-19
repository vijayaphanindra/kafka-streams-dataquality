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

import java.util.Objects;
import java.util.Set;

public class HostStoreInfo {
    private String host;
    private int port;
    private Set<String> storeNames;

    public HostStoreInfo() {

    }

    public HostStoreInfo(final String host, final int port, final Set<String> storeNames) {
        this.host = host;
        this.port = port;
        this.storeNames = storeNames;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public Set<String> getStoreNames() {
        return storeNames;
    }

    public void setStoreNames(Set<String> storeNames) {
        this.storeNames = storeNames;
    }

    @Override
    public String toString() {
        return "HostStoreInfo{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", storeNames=" + storeNames +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HostStoreInfo that = (HostStoreInfo) o;
        return port == that.port &&
                Objects.equals(host, that.host) &&
                Objects.equals(storeNames, that.storeNames);
    }

    @Override
    public int hashCode() {
        return Objects.hash(host, port, storeNames);
    }
}
