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

package io.crate.operation.collect.sources;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import io.crate.metadata.*;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.operation.collect.CollectInputSymbolVisitor;
import io.crate.operation.collect.CrateCollector;
import io.crate.operation.collect.JobCollectContext;
import io.crate.operation.collect.RowsCollector;
import io.crate.operation.projectors.RowReceiver;
import io.crate.operation.reference.information.ColumnContext;
import io.crate.operation.reference.information.InformationReferenceResolver;
import io.crate.planner.node.dql.CollectPhase;
import io.crate.planner.symbol.Literal;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class InformationSchemaCollectSource implements CollectSource {

    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, Iterable<?>> iterables;
    private final ClusterService clusterService;

    @Inject
    protected InformationSchemaCollectSource(Functions functions,
                                             Schemas schemas,
                                             InformationReferenceResolver refResolver,
                                             FulltextAnalyzerResolver ftResolver,
                                             ClusterService clusterService) {
        this.clusterService = clusterService;

        RoutineInfos routineInfos = new RoutineInfos(ftResolver);
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, refResolver);

        Iterable<TableInfo> tablesIterable = FluentIterable.from(schemas)
                .transformAndConcat(new Function<SchemaInfo, Iterable<TableInfo>>() {
                    @Nullable
                    @Override
                    public Iterable<TableInfo> apply(SchemaInfo input) {
                        assert input != null;
                        // filter out partitions
                        return FluentIterable.from(input).filter(new Predicate<TableInfo>() {
                            @Override
                            public boolean apply(TableInfo input) {
                                assert input != null;
                                return !PartitionName.isPartition(input.ident().indexName());
                            }
                        });
                    }
                });
        Iterable<PartitionInfo> tablePartitionsIterable = new PartitionInfos(clusterService);
        Iterable<ColumnContext> columnsIterable = FluentIterable
                .from(tablesIterable)
                .transformAndConcat(new Function<TableInfo, Iterable<ColumnContext>>() {
                    @Nullable
                    @Override
                    public Iterable<ColumnContext> apply(TableInfo input) {
                        assert input != null;
                        return new ColumnsIterator(input);
                    }
                });
        Iterable<TableInfo> tableConstraintsIterable = FluentIterable.from(tablesIterable).filter(new Predicate<TableInfo>() {
            @Override
            public boolean apply(@Nullable TableInfo input) {
                return input != null && input.primaryKey().size() > 0;
            }
        });
        Iterable<RoutineInfo> routinesIterable = FluentIterable.from(routineInfos)
                .filter(new Predicate<RoutineInfo>() {
                    @Override
                    public boolean apply(@Nullable RoutineInfo input) {
                        return input != null;
                    }
                });
        this.iterables = ImmutableMap.<String, Iterable<?>>builder()
                .put("information_schema.tables", tablesIterable)
                .put("information_schema.columns", columnsIterable)
                .put("information_schema.table_constraints", tableConstraintsIterable)
                .put("information_schema.table_partitions", tablePartitionsIterable)
                .put("information_schema.routines", routinesIterable)
                .put("information_schema.schemata", schemas).build();
    }

    class ColumnsIterator implements Iterator<ColumnContext>, Iterable<ColumnContext> {

        private final ColumnContext context = new ColumnContext();
        private final Iterator<ReferenceInfo> columns;

        ColumnsIterator(TableInfo ti) {
            context.ordinal = 0;
            columns = FluentIterable.from(ti).filter(new Predicate<ReferenceInfo>() {
                @Override
                public boolean apply(@Nullable ReferenceInfo input) {
                    return input != null
                            && !input.ident().columnIdent().isSystemColumn()
                            && input.type() != DataTypes.NOT_SUPPORTED;
                }
            }).iterator();
        }

        @Override
        public boolean hasNext() {
            return columns.hasNext();
        }

        @Override
        public ColumnContext next() {
            context.info = columns.next();
            context.ordinal++;
            return context;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("remove not allowed");
        }

        @Override
        public Iterator<ColumnContext> iterator() {
            return this;
        }

    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<CrateCollector> getCollectors(CollectPhase collectPhase, RowReceiver downstream, JobCollectContext jobCollectContext) {
        Routing routing =  collectPhase.routing();
        Map<String, Map<String, List<Integer>>> locations = routing.locations();
        assert locations != null;
        String localNodeId = clusterService.localNode().id();
        assert locations.containsKey(localNodeId);
        assert locations.get(localNodeId).size() == 1;
        String fqTableName = Iterables.getOnlyElement(locations.get(localNodeId).keySet());
        Iterable<?> iterator = iterables.get(fqTableName);
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.extractImplementations(collectPhase);

        Input<Boolean> condition;
        if (collectPhase.whereClause().noMatch()) {
            return ImmutableList.<CrateCollector>of(RowsCollector.empty(downstream));
        }
        if (collectPhase.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectPhase.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        RowsCollector<?> collector = new RowsCollector<>(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterator, condition);
        return ImmutableList.<CrateCollector>of(collector);
    }
}
