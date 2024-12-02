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

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.Index;
import org.jetbrains.annotations.Nullable;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

/**
 * Structure for the 3 different cases:
 *
 *  - ALTER TABLE tbl       -- On a normal table
 *  - ALTER TABLE tbl       -- On a partitioned table
 *  - ALTER TABLE tbl PARTITION (pcol = ?)
 */
public final record AlterTableTarget(RelationName table,
                                     Index[] indices,
                                     List<String> partitionValues,
                                     @Nullable IndexTemplateMetadata templateMetadata) {

    public static AlterTableTarget of(ClusterState state, RelationName table, List<String> partitionValues) {
        Metadata metadata = state.metadata();
        if (partitionValues.isEmpty()) {
            Index[] indices = IndexNameExpressionResolver.concreteIndices(metadata, IndicesOptions.LENIENT_EXPAND_OPEN, table.indexNameOrAlias());
            String templateName = PartitionName.templateName(table.schema(), table.name());
            IndexTemplateMetadata indexTemplateMetadata = metadata.templates().get(templateName);
            return new AlterTableTarget(table, indices, partitionValues, indexTemplateMetadata);
        } else {
            String indexName = new PartitionName(table, partitionValues).asIndexName();
            Index[] indices = IndexNameExpressionResolver.concreteIndices(metadata, IndicesOptions.LENIENT_EXPAND_OPEN, indexName);
            return new AlterTableTarget(table, indices, partitionValues, null);
        }
    }

    public boolean targetsIndividualPartition() {
        return !partitionValues.isEmpty();
    }

    public boolean isEmpty() {
        return indices.length == 0 && templateMetadata == null;
    }
}
