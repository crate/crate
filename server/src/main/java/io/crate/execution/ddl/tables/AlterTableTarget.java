/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.ddl.tables;

import javax.annotation.Nullable;

import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.index.Index;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;

/**
 * Structure for the 3 different cases:
 *
 *  - ALTER TABLE tbl       -- On a normal table
 *  - ALTER TABLE tbl       -- On a partitioned table
 *  - ALTER TABLE tbl PARTITION (pcol = ?)
 */
public final class AlterTableTarget {

    public static AlterTableTarget resolve(IndexNameExpressionResolver indexNameResolver,
                                           ClusterState state,
                                           RelationName table,
                                           @Nullable String partition) {
        if (partition == null) {
            Index[] indices = indexNameResolver.concreteIndices(state, IndicesOptions.lenientExpandOpen(), table.indexNameOrAlias());
            String templateName = PartitionName.templateName(table.schema(), table.name());
            IndexTemplateMetadata indexTemplateMetadata = state.metadata().getTemplates().get(templateName);
            return new AlterTableTarget(indices, indexTemplateMetadata);
        } else {
            Index[] indices = indexNameResolver.concreteIndices(state, IndicesOptions.lenientExpandOpen(), partition);
            return new AlterTableTarget(indices);
        }
    }

    private final Index[] indices;

    @Nullable
    private final IndexTemplateMetadata indexTemplateMetadata;

    public AlterTableTarget(Index[] indices, @Nullable IndexTemplateMetadata indexTemplateMetadata) {
        this.indices = indices;
        this.indexTemplateMetadata = indexTemplateMetadata;
    }

    public AlterTableTarget(Index[] indices) {
        this(indices, null);
    }

    public boolean isEmpty() {
        return indices.length == 0 && indexTemplateMetadata == null;
    }

    public Index[] indices() {
        return indices;
    }

    @Nullable
    public IndexTemplateMetadata templateMetadata() {
        return indexTemplateMetadata;
    }
}
