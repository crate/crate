/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.operation.projectors;

import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegate;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;

import javax.annotation.Nullable;
import java.util.List;

public class ColumnIndexWriterProjector extends AbstractIndexWriterProjector {

    private final List<Input<?>> columnInputs;

    protected ColumnIndexWriterProjector(ClusterService clusterService,
                                         Settings settings,
                                         TransportShardUpsertActionDelegate transportShardUpsertActionDelegate,
                                         TransportCreateIndexAction transportCreateIndexAction,
                                         String tableName,
                                         List<ColumnIdent> primaryKeys,
                                         List<Input<?>> idInputs,
                                         List<Input<?>> partitionedByInputs,
                                         @Nullable ColumnIdent routingIdent,
                                         @Nullable Input<?> routingInput,
                                         List<Reference> columnReferences,
                                         List<Input<?>> columnInputs,
                                         CollectExpression<?>[] collectExpressions,
                                         @Nullable Integer bulkActions,
                                         boolean autoCreateIndices) {
        super(tableName, primaryKeys, idInputs,
                partitionedByInputs, routingIdent, routingInput, collectExpressions);
        assert columnReferences.size() == columnInputs.size();
        this.columnInputs = columnInputs;

        createBulkShardProcessor(
                clusterService,
                settings,
                transportShardUpsertActionDelegate,
                transportCreateIndexAction,
                bulkActions,
                autoCreateIndices,
                null,
                columnReferences.toArray(new Reference[columnReferences.size()]));
    }

    @Override
    protected Symbol[] generateAssignments() {
        return new Symbol[0];
    }

    @Override
    protected Object[] generateMissingAssignments() {
        Object[] missingAssignments = new Object[columnInputs.size()];
        for (int i = 0; i < columnInputs.size(); i++) {
            missingAssignments[i] = columnInputs.get(i).value();
        }
        return missingAssignments;
    }
}
