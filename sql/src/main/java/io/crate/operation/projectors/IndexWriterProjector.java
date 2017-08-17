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

import com.google.common.base.MoreObjects;
import io.crate.analyze.symbol.Symbol;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.executor.transport.ShardUpsertRequest;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.Reference;
import io.crate.operation.NodeJobsCounter;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportBulkCreateIndicesAction;
import org.elasticsearch.action.bulk.BulkRequestExecutor;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.support.XContentMapValues;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;

public class IndexWriterProjector implements Projector {

    private final ShardingUpsertExecutor shardingUpsertExecutor;

    public IndexWriterProjector(ClusterService clusterService,
                                NodeJobsCounter nodeJobsCounter,
                                ScheduledExecutorService scheduler,
                                Functions functions,
                                Settings settings,
                                TransportBulkCreateIndicesAction transportBulkCreateIndicesAction,
                                BulkRequestExecutor<ShardUpsertRequest> shardUpsertAction,
                                Supplier<String> indexNameResolver,
                                Reference rawSourceReference,
                                List<ColumnIdent> primaryKeyIdents,
                                List<? extends Symbol> primaryKeySymbols,
                                @Nullable Symbol routingSymbol,
                                ColumnIdent clusteredByColumn,
                                Input<?> sourceInput,
                                List<? extends CollectExpression<Row, ?>> collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable String[] includes,
                                @Nullable String[] excludes,
                                boolean autoCreateIndices,
                                boolean overwriteDuplicates,
                                UUID jobId) {
        Input<BytesRef> source;
        if (includes == null && excludes == null) {
            //noinspection unchecked
            source = (Input<BytesRef>) sourceInput;
        } else {
            //noinspection unchecked
            source = new MapInput((Input<Map<String, Object>>) sourceInput, includes, excludes);
        }
        RowShardResolver rowShardResolver = new RowShardResolver(functions, primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        ShardUpsertRequest.Builder builder = new ShardUpsertRequest.Builder(
            ShardingUpsertExecutor.BULK_REQUEST_TIMEOUT_SETTING.setting().get(settings),
            overwriteDuplicates,
            true,
            null,
            new Reference[]{rawSourceReference},
            jobId,
            false);

        Function<String, ShardUpsertRequest.Item> itemFactory = id ->
            new ShardUpsertRequest.Item(id, null, new Object[]{source.value()}, null);

        shardingUpsertExecutor = new ShardingUpsertExecutor<>(
            clusterService,
            nodeJobsCounter,
            scheduler,
            MoreObjects.firstNonNull(bulkActions, 100),
            jobId,
            rowShardResolver,
            itemFactory,
            builder::newRequest,
            collectExpressions,
            indexNameResolver,
            autoCreateIndices,
            shardUpsertAction,
            transportBulkCreateIndicesAction
        );
    }


    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.<Row>newInstance(batchIterator, shardingUpsertExecutor);
    }

    @Override
    public boolean providesIndependentScroll() {
        return false;
    }

    private static class MapInput implements Input<BytesRef> {

        private final Input<Map<String, Object>> sourceInput;
        private final String[] includes;
        private final String[] excludes;
        private static final Logger logger = Loggers.getLogger(MapInput.class);
        private int lastSourceSize;

        private MapInput(Input<Map<String, Object>> sourceInput, String[] includes, String[] excludes) {
            this.sourceInput = sourceInput;
            this.includes = includes;
            this.excludes = excludes;
            this.lastSourceSize = BigArrays.BYTE_PAGE_SIZE;
        }

        @Override
        public BytesRef value() {
            Map<String, Object> value = sourceInput.value();
            if (value == null) {
                return null;
            }
            Map<String, Object> filteredMap = XContentMapValues.filter(value, includes, excludes);
            try {
                BytesReference bytes = new XContentBuilder(Requests.INDEX_CONTENT_TYPE.xContent(),
                    new BytesStreamOutput(lastSourceSize)).map(filteredMap).bytes();
                lastSourceSize = bytes.length();
                return bytes.toBytesRef();
            } catch (IOException ex) {
                logger.error("could not parse xContent", ex);
            }
            return null;
        }
    }
}

