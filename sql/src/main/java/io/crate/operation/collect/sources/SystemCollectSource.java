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

package io.crate.operation.collect.sources;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.analyze.EvaluatingNormalizer;
import io.crate.data.RowConsumer;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.information.InformationSchemaTableDefinitions;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogTableDefinitions;
import io.crate.metadata.sys.SysNodeChecksTableInfo;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.sys.SysTableDefinitions;
import io.crate.operation.InputFactory;
import io.crate.operation.collect.BatchIteratorCollectorBridge;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsTransformer;
import io.crate.operation.reference.ReferenceResolver;
import io.crate.operation.reference.StaticTableDefinition;
import io.crate.operation.reference.sys.SysRowUpdater;
import io.crate.operation.reference.sys.check.node.SysNodeChecks;
import io.crate.operation.reference.sys.node.local.NodeSysExpression;
import io.crate.operation.reference.sys.node.local.NodeSysReferenceResolver;
import io.crate.execution.dsl.phases.CollectPhase;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static io.crate.data.SentinelRow.SENTINEL;

/**
 * this collect service can be used to retrieve a collector for system tables (which don't contain shards)
 * <p>
 * System tables are generally represented as Iterable of some type and are converted on-the-fly to {@link Row}
 */
public class SystemCollectSource implements CollectSource {

    private final Functions functions;
    private final NodeSysExpression nodeSysExpression;
    private final ImmutableMap<TableIdent, SysRowUpdater<?>> rowUpdaters;
    private final ClusterService clusterService;
    private final InputFactory inputFactory;

    private final InformationSchemaTableDefinitions informationSchemaTables;
    private final SysTableDefinitions sysTables;
    private final PgCatalogTableDefinitions pgCatalogTables;

    @Inject
    public SystemCollectSource(ClusterService clusterService,
                               Functions functions,
                               NodeSysExpression nodeSysExpression,
                               InformationSchemaTableDefinitions informationSchemaTables,
                               SysTableDefinitions sysTableDefinitions,
                               SysNodeChecks sysNodeChecks,
                               PgCatalogTableDefinitions pgCatalogTables) {
        this.clusterService = clusterService;
        inputFactory = new InputFactory(functions);
        this.functions = functions;
        this.nodeSysExpression = nodeSysExpression;
        this.informationSchemaTables = informationSchemaTables;
        this.sysTables = sysTableDefinitions;
        this.pgCatalogTables = pgCatalogTables;

        rowUpdaters = ImmutableMap.of(SysNodeChecksTableInfo.IDENT, sysNodeChecks);
    }

    Function<Iterable, Iterable<? extends Row>> toRowsIterableTransformation(RoutedCollectPhase collectPhase,
                                                                             ReferenceResolver<?> referenceResolver,
                                                                             boolean requiresRepeat) {
        return objects -> dataIterableToRowsIterable(collectPhase, referenceResolver, requiresRepeat, objects);
    }

    private Iterable<? extends Row> dataIterableToRowsIterable(RoutedCollectPhase collectPhase,
                                                               ReferenceResolver<?> referenceResolver,
                                                               boolean requiresRepeat,
                                                               Iterable<?> data) {
        if (requiresRepeat) {
            data = ImmutableList.copyOf(data);
        }
        return RowsTransformer.toRowsIterable(
            inputFactory,
            referenceResolver,
            collectPhase,
            data);
    }

    @Override
    public CrateCollector getCollector(CollectPhase phase,
                                       RowConsumer consumer,
                                       JobCollectContext jobCollectContext) {
        RoutedCollectPhase collectPhase = (RoutedCollectPhase) phase;
        // sys.operations can contain a _node column - these refs need to be normalized into literals
        EvaluatingNormalizer normalizer = new EvaluatingNormalizer(
            functions, RowGranularity.DOC, new NodeSysReferenceResolver(nodeSysExpression), null);
        final RoutedCollectPhase routedCollectPhase = collectPhase.normalize(normalizer, null);

        boolean requiresScroll = consumer.requiresScroll();

        Map<String, Map<String, List<Integer>>> locations = collectPhase.routing().locations();
        String table = Iterables.getOnlyElement(locations.get(clusterService.localNode().getId()).keySet());
        TableIdent tableIdent = TableIdent.fromIndexName(table);
        StaticTableDefinition<?> tableDefinition = tableDefinition(tableIdent);

        return BatchIteratorCollectorBridge.newInstance(
            () -> tableDefinition.getIterable(routedCollectPhase.user()).get().thenApply(dataIterable ->
                InMemoryBatchIterator.of(
                    dataIterableToRowsIterable(routedCollectPhase,
                        tableDefinition.getReferenceResolver(),
                        requiresScroll, dataIterable),
                    SENTINEL
                )),
            consumer
        );
    }

    public StaticTableDefinition<?> tableDefinition(TableIdent tableIdent) {
        StaticTableDefinition<?> tableDefinition;
        switch (tableIdent.schema()) {
            case InformationSchemaInfo.NAME:
                tableDefinition = informationSchemaTables.get(tableIdent);
                break;
            case SysSchemaInfo.NAME:
                tableDefinition = sysTables.get(tableIdent);
                break;
            case PgCatalogSchemaInfo.NAME:
                tableDefinition = pgCatalogTables.get(tableIdent);
                break;
            default:
                throw new SchemaUnknownException(tableIdent.schema());
        }
        if (tableDefinition == null) {
            throw new TableUnknownException(tableIdent);
        }
        return tableDefinition;
    }

    /**
     * Returns a new updater for a given table.
     *
     * @param ident the ident of the table
     * @return a row updater instance for the given table
     */
    public SysRowUpdater<?> getRowUpdater(TableIdent ident) {
        assert rowUpdaters.containsKey(ident) : "RowUpdater for " + ident.fqn() + " must exist";
        return rowUpdaters.get(ident);
    }
}
