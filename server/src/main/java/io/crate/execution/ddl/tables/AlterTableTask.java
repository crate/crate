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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;

import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.cluster.DDLClusterStateTaskExecutor;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.doc.DocTableInfoFactory;

public class AlterTableTask<T> extends DDLClusterStateTaskExecutor<T> {

    private final NodeContext nodeContext;
    private final RelationName relationName;
    private final AlterTableOperator<T> alterTableOperator;

    public AlterTableTask(NodeContext nodeContext,
                          RelationName relationName,
                          AlterTableOperator<T> alterTableOperator) {
        this.nodeContext = nodeContext;
        this.relationName = relationName;
        this.alterTableOperator = alterTableOperator;
    }

    @Override
    public ClusterState execute(ClusterState currentState, T t) throws Exception {
        DocTableInfoFactory docTableInfoFactory = new DocTableInfoFactory(nodeContext);
        Metadata metadata = currentState.metadata();
        DocTableInfo currentTable = docTableInfoFactory.create(relationName, metadata);
        Metadata.Builder metadataBuilder = Metadata.builder(metadata);
        DocTableInfo newTable = alterTableOperator.apply(t, currentTable, metadataBuilder, nodeContext);
        if (newTable == currentTable) {
            return currentState;
        }
        newTable.writeTo(metadata, metadataBuilder);
        Metadata newMetadata = metadataBuilder.build();
        // Ensure new table can still be parsed
        docTableInfoFactory.create(relationName, newMetadata);
        return ClusterState.builder(currentState)
            .metadata(newMetadata)
            .build();
    }

    public interface AlterTableOperator<T> {
        DocTableInfo apply(T request, DocTableInfo docTableInfo, Metadata.Builder metadataBuilder, NodeContext nodeContext);
    }
}
