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

package io.crate.executor.transport;

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.action.sql.Option;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AlterTableAnalyzedStatement;
import io.crate.analyze.PartitionedTableParameterInfo;
import io.crate.analyze.TableParameter;
import io.crate.core.MultiFutureCallback;
import io.crate.core.collections.Row;
import io.crate.exceptions.AlterTableAliasException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataType;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Stream;

@Singleton
public class AlterTableOperation {

    private static final Function<Object, Long> LONG_NULL_FUNCTION = Functions.constant(null);
    private final ClusterService clusterService;
    private final TransportActionProvider transportActionProvider;
    private final SQLOperations sqlOperations;

    @Inject
    public AlterTableOperation(ClusterService clusterService,
                               TransportActionProvider transportActionProvider,
                               SQLOperations sqlOperations) {
        this.clusterService = clusterService;
        this.transportActionProvider = transportActionProvider;
        this.sqlOperations = sqlOperations;
    }

    public ListenableFuture<Long> executeAlterTableAddColumn(final AddColumnAnalyzedStatement analysis) {
        final SettableFuture<Long> result = SettableFuture.create();
        if (analysis.newPrimaryKeys() || analysis.hasNewGeneratedColumns()) {
            TableIdent ident = analysis.table().ident();
            String stmt =
                String.format(Locale.ENGLISH, "SELECT COUNT(*) FROM \"%s\".\"%s\"", ident.schema(), ident.name());

            SQLOperations.Session session = sqlOperations.createSession(ident.schema(), Option.NONE, 1);
            try {
                session.parse(SQLOperations.Session.UNNAMED, stmt, Collections.<DataType>emptyList());
                session.bind(SQLOperations.Session.UNNAMED, SQLOperations.Session.UNNAMED, Collections.emptyList(), null);
                session.execute(SQLOperations.Session.UNNAMED, 1, new ResultSetReceiver(analysis, result));
                session.sync();
            } catch (Throwable t) {
                result.setException(t);
            }
        } else {
            addColumnToTable(analysis, result);
        }
        return result;
    }

    private class ResultSetReceiver implements ResultReceiver {

        private final AddColumnAnalyzedStatement analysis;
        private final SettableFuture<?> result;

        private long count;

        ResultSetReceiver(AddColumnAnalyzedStatement analysis, SettableFuture<?> result) {
            this.analysis = analysis;
            this.result = result;
        }

        @Override
        public void setNextRow(Row row) {
            count = (long) row.get(0);
        }

        @Override
        public void batchFinished() {
        }

        @Override
        public void allFinished() {
            if (count == 0L) {
                addColumnToTable(analysis, result);
            } else {
                String columnFailure = analysis.newPrimaryKeys() ? "primary key" : "generated";
                fail(new UnsupportedOperationException(String.format(Locale.ENGLISH,
                    "Cannot add a %s column to a table that isn't empty", columnFailure)));
            }
        }

        @Override
        public void fail(@Nonnull Throwable t) {
            result.setException(t);
        }

        @Override
        public ListenableFuture<?> completionFuture() {
            return result;
        }
    }

    public ListenableFuture<Long> executeAlterTable(AlterTableAnalyzedStatement analysis) {
        DocTableInfo table = analysis.table();
        if (table.isAlias() && !table.isPartitioned()) {
            return Futures.immediateFailedFuture(new AlterTableAliasException(table.ident().fqn()));
        }

        List<ListenableFuture<Long>> results = new ArrayList<>(3);

        if (table.isPartitioned()) {
            // create new filtered partition table settings
            PartitionedTableParameterInfo tableSettingsInfo =
                (PartitionedTableParameterInfo) table.tableParameterInfo();
            TableParameter parameterWithFilteredSettings = new TableParameter(
                analysis.tableParameter().settings(),
                tableSettingsInfo.partitionTableSettingsInfo().supportedInternalSettings());

            Optional<PartitionName> partitionName = analysis.partitionName();
            if (partitionName.isPresent()) {
                String index = partitionName.get().asIndexName();
                results.add(updateMapping(analysis.tableParameter().mappings(), index));
                results.add(updateSettings(parameterWithFilteredSettings, index));
            } else {
                // template gets all changes unfiltered
                results.add(updateTemplate(analysis.tableParameter(), table.ident()));

                if (!analysis.excludePartitions()) {
                    String[] indices = Stream.of(table.concreteIndices()).map(Index::getName).toArray(String[]::new);
                    results.add(updateMapping(analysis.tableParameter().mappings(), indices));
                    results.add(updateSettings(parameterWithFilteredSettings, indices));
                }
            }
        } else {
            results.add(updateMapping(analysis.tableParameter().mappings(), table.ident().indexName()));
            results.add(updateSettings(analysis.tableParameter(), table.ident().indexName()));
        }

        final SettableFuture<Long> result = SettableFuture.create();
        applyMultiFutureCallback(result, results);
        return result;
    }

    private ListenableFuture<Long> updateTemplate(TableParameter tableParameter, TableIdent tableIdent) {
        return updateTemplate(tableParameter.mappings(), tableParameter.settings(), tableIdent);
    }

    private ListenableFuture<Long> updateTemplate(Map<String, Object> newMappings,
                                                  Settings newSettings,
                                                  TableIdent tableIdent) {
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
        IndexTemplateMetaData indexTemplateMetaData =
            clusterService.state().metaData().templates().get(templateName);
        if (indexTemplateMetaData == null) {
            return Futures.immediateFailedFuture(new RuntimeException("Template for partitioned table is missing"));
        }

        // merge mappings
        Map<String, Object> mapping = mergeTemplateMapping(indexTemplateMetaData, newMappings);

        // merge settings
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(indexTemplateMetaData.settings());
        settingsBuilder.put(newSettings);

        PutIndexTemplateRequest request = new PutIndexTemplateRequest(templateName)
            .create(false)
            .mapping(Constants.DEFAULT_MAPPING_TYPE, mapping)
            .order(indexTemplateMetaData.order())
            .settings(settingsBuilder.build())
            .template(indexTemplateMetaData.template());
        for (ObjectObjectCursor<String, AliasMetaData> container : indexTemplateMetaData.aliases()) {
            Alias alias = new Alias(container.key);
            request.alias(alias);
        }

        FutureActionListener<PutIndexTemplateResponse, Long> listener = new FutureActionListener<>(LONG_NULL_FUNCTION);
        transportActionProvider.transportPutIndexTemplateAction().execute(request, listener);
        return listener;
    }

    private ListenableFuture<Long> updateMapping(Map<String, Object> newMapping, String... indices) {
        if (newMapping.isEmpty()) {
            return Futures.immediateFuture(null);
        }
        assert areAllMappingsEqual(clusterService.state().metaData(), indices) :
            "Trying to update mapping for indices with different existing mappings";

        Map<String, Object> mapping;
        try {
            MetaData metaData = clusterService.state().metaData();
            String index = indices[0];
            mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
        } catch (IOException e) {
            return Futures.immediateFailedFuture(e);
        }

        XContentHelper.update(mapping, newMapping, false);

        // update mapping of all indices
        PutMappingRequest request = new PutMappingRequest(indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        request.source(mapping);

        FutureActionListener<PutMappingResponse, Long> listener = new FutureActionListener<>(LONG_NULL_FUNCTION);
        transportActionProvider.transportPutMappingAction().execute(request, listener);
        return listener;
    }

    private Map<String, Object> parseMapping(String mappingSource) throws IOException {
        try (XContentParser parser = XContentFactory.xContent(mappingSource).createParser(mappingSource)) {
            return parser.map();
        } catch (IOException e) {
            throw new ElasticsearchException("failed to parse mapping");
        }
    }

    private Map<String, Object> mergeTemplateMapping(IndexTemplateMetaData templateMetaData,
                                                     Map<String, Object> newMapping) {
        Map<String, Object> mergedMapping = new HashMap<>();
        for (ObjectObjectCursor<String, CompressedXContent> cursor : templateMetaData.mappings()) {
            try {
                Map<String, Object> mapping = parseMapping(cursor.value.toString());
                Object o = mapping.get(Constants.DEFAULT_MAPPING_TYPE);
                assert o != null && o instanceof Map :
                    "o must not be null and must be instance of Map";

                XContentHelper.update(mergedMapping, (Map) o, false);
            } catch (IOException e) {
                // pass
            }
        }
        XContentHelper.update(mergedMapping, newMapping, false);
        return mergedMapping;
    }

    private ListenableFuture<Long> updateSettings(TableParameter concreteTableParameter, String... indices) {
        if (concreteTableParameter.settings().getAsMap().isEmpty() || indices.length == 0) {
            return Futures.immediateFuture(null);
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(concreteTableParameter.settings(), indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        FutureActionListener<UpdateSettingsResponse, Long> listener = new FutureActionListener<>(LONG_NULL_FUNCTION);
        transportActionProvider.transportUpdateSettingsAction().execute(request, listener);
        return listener;
    }

    private void addColumnToTable(AddColumnAnalyzedStatement analysis, final SettableFuture<?> result) {
        boolean updateTemplate = analysis.table().isPartitioned();
        List<ListenableFuture<Long>> results = new ArrayList<>(2);
        final Map<String, Object> mapping = analysis.analyzedTableElements().toMapping();

        if (updateTemplate) {
            results.add(updateTemplate(mapping, Settings.EMPTY, analysis.table().ident()));
        }

        String[] indexNames = getIndexNames(analysis.table(), null);
        if (indexNames.length > 0) {
            results.add(updateMapping(mapping, indexNames));
        }

        applyMultiFutureCallback(result, results);
    }

    private void applyMultiFutureCallback(final SettableFuture<?> result, List<ListenableFuture<Long>> futures) {
        MultiFutureCallback<Long> multiFutureCallback = new MultiFutureCallback<>(futures.size(), new FutureCallback<List<Long>>() {
            @Override
            public void onSuccess(@Nullable List<Long> future) {
                result.set(null);
            }

            @Override
            public void onFailure(@Nonnull Throwable t) {
                result.setException(t);
            }
        });
        for (ListenableFuture<Long> future : futures) {
            Futures.addCallback(future, multiFutureCallback);
        }
    }

    private static String[] getIndexNames(DocTableInfo tableInfo, @Nullable PartitionName partitionName) {
        String[] indexNames;
        if (tableInfo.isPartitioned()) {
            if (partitionName == null) {
                // all partitions
                indexNames = Stream.of(tableInfo.concreteIndices()).map(Index::getName).toArray(String[]::new);
            } else {
                // single partition
                indexNames = new String[]{partitionName.asIndexName()};
            }
        } else {
            indexNames = new String[]{tableInfo.ident().indexName()};
        }
        return indexNames;
    }

    private static boolean areAllMappingsEqual(MetaData metaData, String... indices) {
        Map<String, Object> lastMapping = null;
        for (String index : indices) {
            try {
                Map<String, Object> mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
                if (lastMapping != null && !lastMapping.equals(mapping)) {
                    return false;
                }
                lastMapping = mapping;
            } catch (Throwable t) {
                Throwables.propagate(t);
            }
        }
        return true;
    }
}
