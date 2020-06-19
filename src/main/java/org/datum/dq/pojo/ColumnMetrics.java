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
package org.datum.dq.pojo;

import java.util.Objects;

public class ColumnMetrics {
    private String column;
    private int emptyCount;
    private int count;
    private float completeness;

    public ColumnMetrics() {

    }

    public ColumnMetrics(String column, int emptyCount, int count, float completeness) {
        this.column = column;
        this.emptyCount = emptyCount;
        this.count = count;
        this.completeness = completeness;
    }

    public String getColumn() {
        return column;
    }

    public void setColumn(String column) {
        this.column = column;
    }

    public int getEmptyCount() {
        return emptyCount;
    }

    public void setEmptyCount(int emptyCount) {
        this.emptyCount = emptyCount;
    }

    public float getCompleteness() {
        return completeness;
    }

    public void setCompleteness(float completeness) {
        this.completeness = completeness;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ColumnMetrics that = (ColumnMetrics) o;
        return emptyCount == that.emptyCount &&
                count == that.count &&
                Float.compare(that.completeness, completeness) == 0 &&
                Objects.equals(column, that.column);
    }

    @Override
    public int hashCode() {
        return Objects.hash(column, emptyCount, count, completeness);
    }

    @Override
    public String toString() {
        return "ColumnMetrics{" +
                "column='" + column + '\'' +
                ", emptyCount=" + emptyCount +
                ", count=" + count +
                ", completeness=" + completeness +
                '}';
    }
}
