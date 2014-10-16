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

package io.crate.operation.collect;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import io.crate.PartitionName;
import io.crate.metadata.*;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.information.RowCollectExpression;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.operation.projectors.Projector;
import io.crate.operation.reference.information.ColumnContext;
import io.crate.operation.reference.information.InformationDocLevelReferenceResolver;
import io.crate.planner.node.dql.QueryAndFetchNode;
import io.crate.planner.symbol.Literal;
import io.crate.types.DataTypes;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

public class InformationSchemaCollectService implements CollectService {

    private final Iterable<TableInfo> tablesIterable;
    private final Iterable<ColumnContext> columnsIterable;
    private final Iterable<TableInfo> tableConstraintsIterable;
    private final Iterable<RoutineInfo> routinesIterable;

    private final CollectInputSymbolVisitor<RowCollectExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, Iterable<?>> iterables;

    private final Iterable<TablePartitionInfo> tablePartitionsIterable;

    @Inject
    protected InformationSchemaCollectService(Functions functions,
                                              ReferenceInfos referenceInfos,
                                              InformationDocLevelReferenceResolver refResolver,
                                              FulltextAnalyzerResolver ftResolver) {

        RoutineInfos routineInfos = new RoutineInfos(ftResolver);
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions, refResolver);

        tablesIterable = FluentIterable.from(referenceInfos)
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
                                return !PartitionName.isPartition(input.ident().name());
                            }
                        });
                    }
                });
        tablePartitionsIterable = FluentIterable.from(new TablePartitionInfos(tablesIterable));
        columnsIterable = FluentIterable
                .from(tablesIterable)
                .transformAndConcat(new Function<TableInfo, Iterable<ColumnContext>>() {
                    @Nullable
                    @Override
                    public Iterable<ColumnContext> apply(TableInfo input) {
                        assert input != null;
                        return new ColumnsIterator(input);
                    }
                });
        tableConstraintsIterable = FluentIterable.from(tablesIterable).filter(new Predicate<TableInfo>() {
                @Override
                public boolean apply(@Nullable TableInfo input) {
                    return input != null && input.primaryKey().size() > 0;
                }
        });
        routinesIterable = FluentIterable.from(routineInfos)
                .filter(new Predicate<RoutineInfo>() {
                    @Override
                    public boolean apply(@Nullable RoutineInfo input) {
                        return input != null;
                    }
                });
        this.iterables = ImmutableMap.of(
                "information_schema.tables", tablesIterable,
                "information_schema.columns", columnsIterable,
                "information_schema.table_constraints", tableConstraintsIterable,
                "information_schema.table_partitions", tablePartitionsIterable,
                "information_schema.routines", routinesIterable
        );
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
                                && !DocSysColumns.COLUMN_IDENTS.containsKey(input.ident().columnIdent())
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

    static class InformationSchemaCollector<R> implements CrateCollector {

        private final List<Input<?>> inputs;
        private final List<RowCollectExpression<R, ?>> collectorExpressions;
        private Projector downstream;
        private final Iterable<R> rows;
        private final Input<Boolean> condition;

        protected InformationSchemaCollector(List<Input<?>> inputs,
                                             List<RowCollectExpression<R, ?>> collectorExpressions,
                                             Projector downstream,
                                             Iterable<R> rows,
                                             Input<Boolean> condition) {
            this.inputs = inputs;
            this.collectorExpressions = collectorExpressions;
            this.rows = rows;
            this.condition = condition;
            assert downstream != null;
            downstream(downstream);
        }

        @Override
        public void doCollect() throws Exception {
            for (R row : rows) {
                for (RowCollectExpression<R, ?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                Boolean match = condition.value();
                if (match == null || !match) {
                    // no match
                    continue;
               }

                Object[] newRow = new Object[inputs.size()];
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downstream.setNextRow(newRow)) {
                    // no more rows required, we can stop here
                    downstream.upstreamFinished();
                    throw new CollectionTerminatedException();
                }
            }
            downstream.upstreamFinished();
        }

        @Override
        public void downstream(Projector downstream) {
            downstream.registerUpstream(this);
            this.downstream = downstream;
        }

        @Override
        public Projector downstream() {
            return downstream;
        }
    }

    @SuppressWarnings("unchecked")
    public CrateCollector getCollector(QueryAndFetchNode queryAndFetchNode, Projector downstream) {
        if (queryAndFetchNode.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        }
        Routing routing =  queryAndFetchNode.routing();
        assert routing.locations().containsKey(null);
        assert routing.locations().get(null).size() == 1;
        String fqTableName = routing.locations().get(null).keySet().iterator().next();
        Iterable<?> iterator = iterables.get(fqTableName);
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.process(queryAndFetchNode);

        Input<Boolean> condition;
        if (queryAndFetchNode.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(queryAndFetchNode.whereClause().query(), ctx);
        } else {
            condition = Literal.newLiteral(true);
        }

        return new InformationSchemaCollector(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterator, condition);
    }
}
