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
import com.google.common.base.Supplier;
import com.google.common.util.concurrent.Futures;
import io.crate.analyze.symbol.Reference;
import io.crate.analyze.symbol.Symbol;
import io.crate.core.collections.Row;
import io.crate.executor.transport.SymbolBasedShardUpsertRequest;
import io.crate.executor.transport.TransportActionProvider;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.settings.CrateSettings;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.operation.collect.RowShardResolver;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.bulk.BulkRetryCoordinatorPool;
import org.elasticsearch.action.bulk.SymbolBasedBulkShardProcessor;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.logging.ESLogger;
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
import java.util.concurrent.atomic.AtomicBoolean;

public class IndexWriterProjector extends AbstractProjector {

    private final BytesRefGenerator sourceGenerator;
    private final RowShardResolver rowShardResolver;
    private final Supplier<String> indexNameResolver;
    private final Iterable<? extends CollectExpression<Row, ?>> collectExpressions;
    private final SymbolBasedBulkShardProcessor<SymbolBasedShardUpsertRequest> bulkShardProcessor;
    private final AtomicBoolean failed = new AtomicBoolean(false);

    public IndexWriterProjector(ClusterService clusterService,
                                Settings settings,
                                TransportActionProvider transportActionProvider,
                                Supplier<String> indexNameResolver,
                                BulkRetryCoordinatorPool bulkRetryCoordinatorPool,
                                Reference rawSourceReference,
                                List<ColumnIdent> primaryKeyIdents,
                                List<Symbol> primaryKeySymbols,
                                @Nullable Symbol routingSymbol,
                                ColumnIdent clusteredByColumn,
                                Input<?> sourceInput,
                                Iterable<? extends CollectExpression<Row, ?>> collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable String[] includes,
                                @Nullable String[] excludes,
                                boolean autoCreateIndices,
                                boolean overwriteDuplicates,
                                UUID jobId) {
        this.indexNameResolver = indexNameResolver;
        this.collectExpressions = collectExpressions;
        if (includes == null && excludes == null) {
            //noinspection unchecked
            sourceGenerator = new BytesRefInput((Input<BytesRef>) sourceInput);
        } else {
            //noinspection unchecked
            sourceGenerator =
                    new MapInput((Input<Map<String, Object>>) sourceInput, includes, excludes);
        }
        rowShardResolver = new RowShardResolver(primaryKeyIdents, primaryKeySymbols, clusteredByColumn, routingSymbol);
        SymbolBasedShardUpsertRequest.Builder builder = new SymbolBasedShardUpsertRequest.Builder(
                CrateSettings.BULK_REQUEST_TIMEOUT.extractTimeValue(settings),
                overwriteDuplicates,
                true,
                null,
                new Reference[]{rawSourceReference},
                jobId);

        bulkShardProcessor = new SymbolBasedBulkShardProcessor<>(
                clusterService,
                transportActionProvider.transportBulkCreateIndicesAction(),
                settings,
                bulkRetryCoordinatorPool,
                autoCreateIndices,
                MoreObjects.firstNonNull(bulkActions, 100),
                builder,
                transportActionProvider.symbolBasedTransportShardUpsertActionDelegate(),
                jobId
        );
    }

    @Override
    public void downstream(RowReceiver rowReceiver) {
        super.downstream(rowReceiver);
        Futures.addCallback(bulkShardProcessor.result(), new BulkProcessorFutureCallback(failed, rowReceiver));
    }

    @Override
    public boolean setNextRow(Row row) {
        for (CollectExpression<Row, ?> collectExpression : collectExpressions) {
            collectExpression.setNextRow(row);
        }
        rowShardResolver.setNextRow(row);
        return bulkShardProcessor.add(
                indexNameResolver.get(),
                rowShardResolver.id(),
                new Object[] { sourceGenerator.generateSource() },
                rowShardResolver.routing(),
                null);
    }

    @Override
    public void finish() {
        bulkShardProcessor.close();
    }

    @Override
    public void fail(Throwable throwable) {
        failed.set(true);
        downstream.fail(throwable);
        bulkShardProcessor.kill(throwable);
    }

    private interface BytesRefGenerator {
        BytesRef generateSource();
    }

    private static class BytesRefInput implements BytesRefGenerator {
        private final Input<BytesRef> input;

        private BytesRefInput(Input<BytesRef> input) {
            this.input = input;
        }

        @Override
        public BytesRef generateSource() {
            return input.value();
        }
    }

    private static class MapInput implements BytesRefGenerator {

        private final Input<Map<String, Object>> sourceInput;
        private final String[] includes;
        private final String[] excludes;
        private static final ESLogger logger = Loggers.getLogger(MapInput.class);
        private int lastSourceSize;

        private MapInput(Input<Map<String, Object>> sourceInput, String[] includes, String[] excludes) {
            this.sourceInput = sourceInput;
            this.includes = includes;
            this.excludes = excludes;
            this.lastSourceSize = BigArrays.BYTE_PAGE_SIZE;
        }

        @Override
        public BytesRef generateSource() {
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

