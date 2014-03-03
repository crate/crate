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

package io.crate.operator.operations.collect;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.Functions;
import io.crate.metadata.HandlerSideRouting;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.information.InformationCollectorExpression;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operator.Input;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.operations.CollectInputSymbolVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.operator.reference.information.ColumnContext;
import io.crate.operator.reference.information.InformationDocLevelReferenceResolver;
import io.crate.planner.node.dql.CollectNode;
import io.crate.planner.symbol.BooleanLiteral;
import org.apache.lucene.search.CollectionTerminatedException;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

public class InformationSchemaCollectService implements CollectService {

    private final Iterable<TableInfo> tablesIterable;
    private final Iterable<ColumnContext> columnsIterable;

    private final CollectInputSymbolVisitor<InformationCollectorExpression<?, ?>> docInputSymbolVisitor;
    private final ImmutableMap<String, Iterable<?>> iterables;

    @Inject
    protected InformationSchemaCollectService(Functions functions, ReferenceInfos referenceInfos) {

        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions,
                InformationDocLevelReferenceResolver.INSTANCE);

        tablesIterable = FluentIterable.from(referenceInfos)
                .transformAndConcat(new Function<SchemaInfo, Iterable<TableInfo>>() {
                    @Nullable
                    @Override
                    public Iterable<TableInfo> apply(@Nullable SchemaInfo input) {
                        return input;
                    }
                });
        columnsIterable = FluentIterable
                .from(tablesIterable)
                .transformAndConcat(new Function<TableInfo, Iterable<ColumnContext>>() {
                    @Nullable
                    @Override
                    public Iterable<ColumnContext> apply(@Nullable TableInfo input) {
                        return new ColumnsIterator(input);
                    }
                });
        this.iterables = ImmutableMap.<String, Iterable<?>>of(
                "tables", tablesIterable,
                "columns", columnsIterable
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
                                && !DocSysColumns.columnIdents.containsKey(input.ident().columnIdent())
                                && input.type() != DataType.NOT_SUPPORTED;
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
        private final List<InformationCollectorExpression<R, ?>> collectorExpressions;
        private final Projector downStream;
        private final Iterable<R> rows;
        private final Input<Boolean> condition;

        protected InformationSchemaCollector(List<Input<?>> inputs,
                                             List<InformationCollectorExpression<R, ?>> collectorExpressions,
                                             Projector downStream,
                                             Iterable<R> rows,
                                             Input<Boolean> condition) {
            this.inputs = inputs;
            this.collectorExpressions = collectorExpressions;
            this.downStream = downStream;
            this.rows = rows;
            this.condition = condition;
        }

        @Override
        public void doCollect() throws Exception {
            for (R row : rows) {
                for (InformationCollectorExpression<R, ?> collectorExpression : collectorExpressions) {
                    collectorExpression.setNextRow(row);
                }
                if (!condition.value()) {
                    // no match
                    continue;
                }

                Object[] newRow = new Object[inputs.size()];
                int i = 0;
                for (Input<?> input : inputs) {
                    newRow[i++] = input.value();
                }
                if (!downStream.setNextRow(newRow)) {
                    // no more rows required, we can stop here
                    throw new CollectionTerminatedException();
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    public CrateCollector getCollector(CollectNode collectNode, Projector downstream) {
        assert collectNode.routing() instanceof HandlerSideRouting;
        if (collectNode.whereClause().noMatch()) {
            return CrateCollector.NOOP;
        }
        HandlerSideRouting routing = (HandlerSideRouting) collectNode.routing();
        Iterable<?> iterator = iterables.get(routing.tableIdent().name());
        CollectInputSymbolVisitor.Context ctx = docInputSymbolVisitor.process(collectNode);

        Input<Boolean> condition;
        if (collectNode.whereClause().hasQuery()) {
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = BooleanLiteral.TRUE;
        }

        return new InformationSchemaCollector(
                ctx.topLevelInputs(), ctx.docLevelExpressions(), downstream, iterator, condition);
    }
}