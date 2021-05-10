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

package io.crate.metadata.doc;

import io.crate.exceptions.RelationUnknown;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;

import java.util.Map;

public class TestingDocTableInfoFactory implements DocTableInfoFactory {

    private final Map<RelationName, DocTableInfo> tables;
    private final InternalDocTableInfoFactory internalFactory;

    public TestingDocTableInfoFactory(Map<RelationName, DocTableInfo> tables) {
        this.tables = tables;
        this.internalFactory = null;
    }

    public TestingDocTableInfoFactory(Map<RelationName, DocTableInfo> tables,
                                      NodeContext nodeCtx,
                                      IndexNameExpressionResolver indexNameExpressionResolver) {
        this.tables = tables;
        this.internalFactory = new InternalDocTableInfoFactory(nodeCtx, indexNameExpressionResolver);
    }

    @Override
    public DocTableInfo create(RelationName ident, ClusterState state) {
        DocTableInfo tableInfo = tables.get(ident);
        if (tableInfo == null) {
            if (internalFactory == null) {
                throw new RelationUnknown(ident);
            }
            return internalFactory.create(ident, state);
        }
        return tableInfo;
    }
}
