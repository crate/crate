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

package io.crate.execution.ddl.tables;

import java.util.List;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;

import io.crate.metadata.RelationName;

/**
 * Structure for the 3 different cases:
 *
 *  - ALTER TABLE tbl       -- On a normal table
 *  - ALTER TABLE tbl       -- On a partitioned table
 *  - ALTER TABLE tbl PARTITION (pcol = ?)
 */
public final record AlterTableTarget(RelationName table,
                                     List<Index> indices,
                                     List<String> partitionValues) {

    public static AlterTableTarget of(ClusterState state, RelationName table, List<String> partitionValues) {
        Metadata metadata = state.metadata();
        List<Index> indices = metadata.getIndices(table, partitionValues, false, IndexMetadata::getIndex);
        return new AlterTableTarget(table, indices, partitionValues);
    }
}
