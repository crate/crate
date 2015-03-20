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

import io.crate.core.collections.Buckets;
import io.crate.core.collections.Row;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.TableIdent;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectExpression;
import io.crate.planner.symbol.InputColumn;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.bulk.TransportShardUpsertActionDelegate;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexWriterProjector extends AbstractIndexWriterProjector {

    private final SourceInjectorRow row;

    private static final class SourceInjectorRow implements Row {

        private final int idx;
        private final BytesRefGenerator generator;
        Row delegate = null;

        private SourceInjectorRow(int idx, BytesRefGenerator generator) {
            this.idx = idx;
            this.generator = generator;
        }


        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public Object get(int index) {
            if (index == idx){
                return generator.generateSource();
            }
            return delegate.get(index);
        }

        @Override
        public Object[] materialize() {
            return Buckets.materialize(delegate);
        }
    };


    public IndexWriterProjector(ClusterService clusterService,
                                Settings settings,
                                TransportShardUpsertActionDelegate transportShardUpsertActionDelegate,
                                TransportCreateIndexAction transportCreateIndexAction,
                                TableIdent tableIdent,
                                @Nullable String partitionIdent,
                                Reference rawSourceReference,
                                List<ColumnIdent> primaryKeyIdents,
                                List<Symbol> primaryKeySymbols,
                                List<Input<?>> partitionedByInputs,
                                @Nullable Symbol routingSymbol,
                                Input<?> sourceInput,
                                InputColumn sourceInputColumn,
                                CollectExpression<?>[] collectExpressions,
                                @Nullable Integer bulkActions,
                                @Nullable String[] includes,
                                @Nullable String[] excludes,
                                boolean autoCreateIndices,
                                boolean overwriteDuplicates) {
        super(tableIdent, partitionIdent, primaryKeyIdents, primaryKeySymbols, partitionedByInputs,
                routingSymbol, collectExpressions);

        if (includes == null && excludes == null) {
            //noinspection unchecked
            row = new SourceInjectorRow(sourceInputColumn.index(), new BytesRefInput((Input<BytesRef>) sourceInput));
        } else {
            //noinspection unchecked
            row = new SourceInjectorRow(sourceInputColumn.index(),
                    new MapInput((Input<Map<String, Object>>) sourceInput, includes, excludes));
        }

        Map<Reference, Symbol> insertAssignments = new HashMap<>(1);
        insertAssignments.put(rawSourceReference, sourceInputColumn);

        createBulkShardProcessor(
                clusterService,
                settings,
                transportShardUpsertActionDelegate,
                transportCreateIndexAction,
                bulkActions,
                autoCreateIndices,
                overwriteDuplicates,
                null,
                insertAssignments);
    }

    @Override
    protected Row updateRow(Row row) {
        this.row.delegate = row;
        return this.row;
    }

    private interface BytesRefGenerator {
        public BytesRef generateSource();
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

