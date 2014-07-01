/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.metadata.ColumnIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardBulkAction;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class IndexWriterProjector extends AbstractIndexWriterProjector {

    private final ESLogger logger = Loggers.getLogger(getClass());

    private final Input<?> sourceInput;

    private final @Nullable String[] includes;
    private final @Nullable String[] excludes;

    public IndexWriterProjector(ClusterService clusterService,
                                Settings settings,
                                TransportShardBulkAction transportShardBulkAction,
                                TransportCreateIndexAction transportCreateIndexAction,
                                String tableName,
                                List<ColumnIdent> primaryKeys,
                                List<Input<?>> idInputs,
                                List<Input<?>> partitionedByInputs,
                                @Nullable ColumnIdent routingIdent,
                                @Nullable Input<?> routingInput,
                                Input<?> sourceInput,
                                CollectExpression<?>[] collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable String[] includes,
                                @Nullable String[] excludes) {
        super(clusterService, settings, transportShardBulkAction, transportCreateIndexAction,
                tableName, primaryKeys, idInputs, partitionedByInputs,
                routingIdent, routingInput,
                collectExpressions, bulkActions);
        this.sourceInput = sourceInput;
        this.includes = includes;
        this.excludes = excludes;
    }

    public BytesReference generateSource() {
        Object value = sourceInput.value();
        if (value == null) {
            return null;
        }
        if (includes != null || excludes != null) {
            assert value instanceof Map;
            // exclude partitioned columns from source
            Map<String, Object> sourceAsMap = XContentMapValues.filter((Map) value, includes, excludes);
            try {
                return XContentFactory.contentBuilder(Requests.INDEX_CONTENT_TYPE).map(sourceAsMap).bytes();
            } catch (IOException e) {
                logger.error("Could not parse xContent", e);
                return null;
            }
        }
        assert value instanceof BytesRef;
        return new BytesArray((BytesRef) value);
    }
}

