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

import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

public record SysSnapshot(String uuid,
                          String name,
                          String repository,
                          List<String> concreteIndices,
                          List<String> partitionedTables,
                          Long started,
                          Long finished,
                          String version,
                          String state,
                          List<String> failures,
                          String reason,
                          int totalShards,
                          Boolean includeGlobalState) {


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
            .map(IndexName::decode)
            .filter(IndexParts::isPartitioned)
            .map(indexParts -> new PartitionName(
                new RelationName(indexParts.schema(), indexParts.table()),
                indexParts.partitionIdent()))
            .toList();
    }
}
