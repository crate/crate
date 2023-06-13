/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata;

import io.crate.metadata.table.StoredTable;
import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;

import org.jetbrains.annotations.Nullable;

import java.util.Map;

public class PartitionInfo implements StoredTable {

    private final PartitionName name;
    private final int numberOfShards;
    private final String numberOfReplicas;
    private final Version versionCreated;
    private final Version versionUpgraded;
    private final boolean closed;
    private final Map<String, Object> values;
    private final Settings tableParameters;

    public PartitionInfo(PartitionName name,
                         int numberOfShards,
                         String numberOfReplicas,
                         @Nullable Version versionCreated,
                         @Nullable Version versionUpgraded,
                         boolean closed,
                         Map<String, Object> values,
                         Settings tableParameters) {
        this.name = name;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.versionCreated = versionCreated;
        this.versionUpgraded = versionUpgraded;
        this.closed = closed;
        this.values = values;
        this.tableParameters = tableParameters;
    }

    public PartitionName name() {
        return name;
    }

    public int numberOfShards() {
        return numberOfShards;
    }

    public String numberOfReplicas() {
        return numberOfReplicas;
    }

    public boolean isClosed() {
        return closed;
    }

    public Map<String, Object> values() {
        return values;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        PartitionInfo that = (PartitionInfo) o;

        if (!name.equals(that.name)) return false;
        if (numberOfReplicas != that.numberOfReplicas) return false;
        if (numberOfShards != that.numberOfShards) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = name.hashCode();
        result = 31 * result + numberOfShards;
        result = 31 * result + numberOfReplicas.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "PartitionInfo{"
            + "name=" + name
            + ", numberOfShards=" + numberOfShards
            + ", numberOfReplicas=" + numberOfReplicas
            + ", versionCreated=" + versionCreated
            + ", versionUpgraded=" + versionUpgraded
            + ", closed=" + closed
            + "}";
    }

    public Settings tableParameters() {
        return tableParameters;
    }

    @Override
    @Nullable
    public Version versionCreated() {
        return versionCreated;
    }

    @Override
    @Nullable
    public Version versionUpgraded() {
        return versionUpgraded;
    }
}

