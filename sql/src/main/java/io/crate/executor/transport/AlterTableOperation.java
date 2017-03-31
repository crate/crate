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
import com.google.common.base.Throwables;
import io.crate.Constants;
import io.crate.action.FutureActionListener;
import io.crate.action.sql.ResultReceiver;
import io.crate.action.sql.SQLOperations;
import io.crate.analyze.AddColumnAnalyzedStatement;
import io.crate.analyze.AlterTableAnalyzedStatement;
import io.crate.analyze.PartitionedTableParameterInfo;
import io.crate.analyze.TableParameter;
import io.crate.concurrent.CompletableFutures;
import io.crate.concurrent.MultiBiConsumer;
import io.crate.data.Row;
import io.crate.exceptions.AlterTableAliasException;
import io.crate.metadata.PartitionName;
import io.crate.metadata.TableIdent;
import io.crate.metadata.doc.DocTableInfo;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateRequest;
import org.elasticsearch.action.admin.indices.template.put.PutIndexTemplateResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.metadata.AliasMetaData;
import org.elasticsearch.cluster.metadata.IndexTemplateMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;

@Singleton
public class AlterTableOperation {

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

    public CompletableFuture<Long> executeAlterTableAddColumn(final AddColumnAnalyzedStatement analysis) {
        final CompletableFuture<Long> result = new CompletableFuture<>();
        if (analysis.newPrimaryKeys() || analysis.hasNewGeneratedColumns()) {
            TableIdent ident = analysis.table().ident();
            String stmt =
                String.format(Locale.ENGLISH, "SELECT COUNT(*) FROM \"%s\".\"%s\"", ident.schema(), ident.name());

            SQLOperations.SQLDirectExecutor sqlDirectExecutor = sqlOperations.createSQLDirectExecutor(
                null,
                SQLOperations.Session.UNNAMED,
                stmt,
                1);
            try {
                sqlDirectExecutor.execute(new ResultSetReceiver(analysis, result), Collections.emptyList());
            } catch (Throwable t) {
                result.completeExceptionally(t);
            }
        } else {
            addColumnToTable(analysis, result);
        }
        return result;
    }

    private class ResultSetReceiver implements ResultReceiver {

        private final AddColumnAnalyzedStatement analysis;
        private final CompletableFuture<?> result;

        private long count;

        ResultSetReceiver(AddColumnAnalyzedStatement analysis, CompletableFuture<?> result) {
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
        public void allFinished(boolean interrupted) {
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
            result.completeExceptionally(t);
        }

        @Override
        public CompletableFuture<?> completionFuture() {
            return result;
        }
    }

    public CompletableFuture<Long> executeAlterTable(AlterTableAnalyzedStatement analysis) {
        DocTableInfo table = analysis.table();
        if (table.isAlias() && !table.isPartitioned()) {
            return CompletableFutures.failedFuture(new AlterTableAliasException(table.ident().fqn()));
        }

        List<CompletableFuture<Long>> results = new ArrayList<>(3);

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
                    String[] indices = table.concreteIndices();
                    results.add(updateMapping(analysis.tableParameter().mappings(), indices));
                    results.add(updateSettings(parameterWithFilteredSettings, indices));
                }
            }
        } else {
            results.add(updateMapping(analysis.tableParameter().mappings(), table.ident().indexName()));
            results.add(updateSettings(analysis.tableParameter(), table.ident().indexName()));
        }

        final CompletableFuture<Long> result = new CompletableFuture<>();
        applyMultiFutureCallback(result, results);
        return result;
    }

    private CompletableFuture<Long> updateTemplate(TableParameter tableParameter, TableIdent tableIdent) {
        return updateTemplate(tableParameter.mappings(), tableParameter.settings(), tableIdent);
    }

    private CompletableFuture<Long> updateTemplate(Map<String, Object> newMappings,
                                                   Settings newSettings,
                                                   TableIdent tableIdent) {
        String templateName = PartitionName.templateName(tableIdent.schema(), tableIdent.name());
        IndexTemplateMetaData indexTemplateMetaData =
            clusterService.state().metaData().templates().get(templateName);
        if (indexTemplateMetaData == null) {
            return CompletableFutures.failedFuture(new RuntimeException("Template for partitioned table is missing"));
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

        FutureActionListener<PutIndexTemplateResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportActionProvider.transportPutIndexTemplateAction().execute(request, listener);
        return listener;
    }

    private CompletableFuture<Long> updateMapping(Map<String, Object> newMapping, String... indices) {
        if (newMapping.isEmpty()) {
            return CompletableFuture.completedFuture(null);
        }
        assert areAllMappingsEqual(clusterService.state().metaData(), indices) :
            "Trying to update mapping for indices with different existing mappings";

        Map<String, Object> mapping;
        try {
            MetaData metaData = clusterService.state().metaData();
            String index = indices[0];
            mapping = metaData.index(index).mapping(Constants.DEFAULT_MAPPING_TYPE).getSourceAsMap();
        } catch (IOException e) {
            return CompletableFutures.failedFuture(e);
        }

        XContentHelper.update(mapping, newMapping, false);

        // update mapping of all indices
        PutMappingRequest request = new PutMappingRequest(indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());
        request.type(Constants.DEFAULT_MAPPING_TYPE);
        request.source(mapping);

        FutureActionListener<PutMappingResponse, Long> listener = new FutureActionListener<>(r -> 0L);
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

    private CompletableFuture<Long> updateSettings(TableParameter concreteTableParameter, String... indices) {
        if (concreteTableParameter.settings().getAsMap().isEmpty() || indices.length == 0) {
            return CompletableFuture.completedFuture(null);
        }
        UpdateSettingsRequest request = new UpdateSettingsRequest(concreteTableParameter.settings(), indices);
        request.indicesOptions(IndicesOptions.lenientExpandOpen());

        FutureActionListener<UpdateSettingsResponse, Long> listener = new FutureActionListener<>(r -> 0L);
        transportActionProvider.transportUpdateSettingsAction().execute(request, listener);
        return listener;
    }

    private void addColumnToTable(AddColumnAnalyzedStatement analysis, final CompletableFuture<?> result) {
        boolean updateTemplate = analysis.table().isPartitioned();
        List<CompletableFuture<Long>> results = new ArrayList<>(2);
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

    private void applyMultiFutureCallback(final CompletableFuture<?> result, List<CompletableFuture<Long>> futures) {
        BiConsumer<List<Long>, Throwable> finalConsumer = (List<Long> receivedResult, Throwable t) -> {
            if (t == null) {
                result.complete(null);
            } else {
                result.completeExceptionally(t);
            }
        };

        MultiBiConsumer<Long> consumer = new MultiBiConsumer<>(futures.size(), finalConsumer);
        for (CompletableFuture<Long> future : futures) {
            future.whenComplete(consumer);
        }
    }

    private static String[] getIndexNames(DocTableInfo tableInfo, @Nullable PartitionName partitionName) {
        String[] indexNames;
        if (tableInfo.isPartitioned()) {
            if (partitionName == null) {
                // all partitions
                indexNames = tableInfo.concreteIndices();
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
