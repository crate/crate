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

package io.crate.expression.reference.sys.snapshot;

import java.util.List;
import java.util.stream.Stream;

import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public class SysSnapshot {

    private final String uuid;
    private final String name;
    private final String repository;
    private final List<String> concreteIndices;
    private final List<String> partitionedTables;
    private final Long started;
    private final Long finished;
    private final String version;
    private final String state;

    private final List<String> snapshotShardFailures;

    public SysSnapshot(String uuid,
                       String name,
                       String repository,
                       List<String> concreteIndices,
                       List<String> partitionedTables,
                       Long started,
                       Long finished,
                       String version,
                       String state,
                       List<String> snapshotShardFailures) {
        this.uuid = uuid;
        this.name = name;
        this.repository = repository;
        this.concreteIndices = concreteIndices;
        this.partitionedTables = partitionedTables;
        this.started = started;
        this.finished = finished;
        this.version = version;
        this.state = state;
        this.snapshotShardFailures = snapshotShardFailures;
    }

    public String uuid() {
        return uuid;
    }

    public String name() {
        return name;
    }

    public String repository() {
        return repository;
    }

    public List<String> concreteIndices() {
        return concreteIndices;
    }

    public Long started() {
        return started;
    }

    public Long finished() {
        return finished;
    }

    public String version() {
        return version;
    }

    public String state() {
        return state;
    }

    public List<String> failures() {
        return snapshotShardFailures;
    }

    public List<String> tables() {
        return Stream.concat(concreteIndices.stream().map(RelationName::fqnFromIndexName), partitionedTables.stream())
            .distinct()
            .toList();
    }

    public List<RelationName> relationNames() {
        return Stream.concat(
                concreteIndices.stream().map(RelationName::fromIndexName),
                partitionedTables.stream().map(RelationName::fromIndexName)
            )
            .distinct()
            .toList();
    }

    public List<PartitionName> tablePartitions() {
        return concreteIndices.stream()
            .map(IndexParts::new)
            .filter(IndexParts::isPartitioned)
            .map(indexParts -> new PartitionName(
                new RelationName(indexParts.getSchema(), indexParts.getTable()),
                indexParts.getPartitionIdent()))
            .toList();
    }
}
