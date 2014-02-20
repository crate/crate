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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterators;
import io.crate.metadata.Functions;
import io.crate.metadata.HandlerSideRouting;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.information.InformationCollectorExpression;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operator.Input;
import io.crate.operator.collector.CrateCollector;
import io.crate.operator.operations.CollectInputSymbolVisitor;
import io.crate.operator.projectors.Projector;
import io.crate.operator.reference.information.InformationDocLevelReferenceResolver;
import io.crate.planner.node.CollectNode;
import io.crate.planner.symbol.BooleanLiteral;
import org.apache.lucene.search.CollectionTerminatedException;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.Iterator;
import java.util.List;

public class InformationSchemaCollectService {

    private final CollectInputSymbolVisitor<InformationCollectorExpression<?, ?>> docInputSymbolVisitor;
    private final InformationSchemaInfo schemaInfo;
    private final ReferenceInfos referenceInfos;
    private final ImmutableMap<String, Iterable<?>> iterables;

    @Inject
    protected InformationSchemaCollectService(Functions functions, InformationSchemaInfo schemaInfo, ReferenceInfos referenceInfos) {
        this.schemaInfo = schemaInfo;
        this.referenceInfos = referenceInfos;
        this.docInputSymbolVisitor = new CollectInputSymbolVisitor<>(functions,
                InformationDocLevelReferenceResolver.INSTANCE);
        this.iterables = ImmutableMap.<String, Iterable<?>>of(
                "tables", new TablesIterator()
        );

    }

    class TablesIterator implements Iterable<TableInfo> {

        private final Iterator<Iterator<TableInfo>> iterators;

        TablesIterator() {
            iterators = Iterators.transform(referenceInfos.iterator(), new Function<SchemaInfo, Iterator<TableInfo>>() {
                @Nullable
                @Override
                public Iterator<TableInfo> apply(@Nullable SchemaInfo input) {
                    return input.iterator();
                }
            });
        }

        @Override
        public Iterator<TableInfo> iterator() {
            return Iterators.concat(iterators);
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
                if (!condition.value()){
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
        if (collectNode.whereClause().hasQuery()){
            // TODO: single arg method
            condition = (Input<Boolean>) docInputSymbolVisitor.process(collectNode.whereClause().query(), ctx);
        } else {
            condition = BooleanLiteral.TRUE;
        }

        return new InformationSchemaCollector(
                ctx.topLevelInputs(),ctx.docLevelExpressions(), downstream, iterator, condition);
    }
}