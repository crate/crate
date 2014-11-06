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
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.action.bulk.TransportShardBulkActionDelegate;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class ColumnIndexWriterProjector extends AbstractIndexWriterProjector {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final List<Input<?>> columnInputs;
    private final List<ColumnIdent> columnIdents;

    protected ColumnIndexWriterProjector(ClusterService clusterService,
                                         Settings settings,
                                         TransportShardBulkActionDelegate transportShardBulkActionDelegate,
                                         TransportCreateIndexAction transportCreateIndexAction,
                                         String tableName,
                                         List<ColumnIdent> primaryKeys,
                                         List<Input<?>> idInputs,
                                         List<Input<?>> partitionedByInputs,
                                         @Nullable ColumnIdent routingIdent,
                                         @Nullable Input<?> routingInput,
                                         List<ColumnIdent> columnIdents,
                                         List<Input<?>> columnInputs,
                                         CollectExpression<?>[] collectExpressions,
                                         @Nullable Integer bulkActions,
                                         boolean autoCreateIndices) {
        super(clusterService, settings, transportShardBulkActionDelegate,
                transportCreateIndexAction, tableName, primaryKeys, idInputs,
                partitionedByInputs, routingIdent, routingInput, collectExpressions,
                bulkActions, autoCreateIndices);
        assert columnIdents.size() == columnInputs.size();
        this.columnIdents = columnIdents;
        this.columnInputs = columnInputs;
    }

    @Override
    protected BytesReference generateSource() {
        Map<String, Object> sourceMap = new HashMap<>(this.columnInputs.size());
        Iterator<ColumnIdent> identIterator = columnIdents.iterator();
        Iterator<Input<?>> inputIterator = columnInputs.iterator();
        while (identIterator.hasNext()) {
            sourceMap.put(identIterator.next().fqn(), inputIterator.next().value());
        }

        try {
            return XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE).map(sourceMap).bytes();
        } catch (IOException e) {
            logger.error("Could not parse xContent", e);
            return null;
        }
    }
}
