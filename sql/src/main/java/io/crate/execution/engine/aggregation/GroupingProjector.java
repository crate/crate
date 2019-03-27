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

package io.crate.execution.engine.aggregation;

import io.crate.breaker.RamAccountingContext;
import io.crate.data.BatchIterator;
import io.crate.data.CollectingBatchIterator;
import io.crate.data.Input;
import io.crate.data.Projector;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Symbol;
import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;

import java.util.List;
import java.util.stream.Collector;

import static io.crate.expression.symbol.Symbols.typeView;

public class GroupingProjector implements Projector {

    private final Collector<Row, ?, Iterable<Row>> collector;


    public GroupingProjector(List<? extends Symbol> keys,
                             List<Input<?>> keyInputs,
                             CollectExpression<Row, ?>[] collectExpressions,
                             AggregateMode mode,
                             AggregationContext[] aggregations,
                             RamAccountingContext ramAccountingContext,
                             Version indexVersionCreated,
                             BigArrays bigArrays) {
        assert keys.size() == keyInputs.size() : "number of key types must match with number of key inputs";
        ensureAllTypesSupported(keys);

        AggregationFunction[] functions = new AggregationFunction[aggregations.length];
        Input[][] inputs = new Input[aggregations.length][];
        for (int i = 0; i < aggregations.length; i++) {
            AggregationContext aggregation = aggregations[i];
            functions[i] = aggregation.function();
            inputs[i] = aggregation.inputs();
        }
        if (keys.size() == 1) {
            Symbol key = keys.get(0);
            if (DataTypes.NUMERIC_PRIMITIVE_TYPES.contains(key.valueType()) &&
                !key.valueType().equals(DataTypes.FLOAT) &&
                !key.valueType().equals(DataTypes.DOUBLE)) {
                collector = new GroupBySingleNumberCollector(
                    key.valueType(),
                    collectExpressions,
                    mode,
                    functions,
                    inputs,
                    ramAccountingContext,
                    keyInputs.get(0),
                    indexVersionCreated,
                    bigArrays
                );
            } else {
                collector = GroupingCollector.singleKey(
                    collectExpressions,
                    mode,
                    functions,
                    inputs,
                    ramAccountingContext,
                    keyInputs.get(0),
                    key.valueType(),
                    indexVersionCreated,
                    bigArrays
                );
            }
        } else {
            //noinspection unchecked
            collector = (GroupingCollector<Object>) (GroupingCollector) GroupingCollector.manyKeys(
                collectExpressions,
                mode,
                functions,
                inputs,
                ramAccountingContext,
                keyInputs,
                typeView(keys),
                indexVersionCreated,
                bigArrays
            );
        }
    }

    private static void ensureAllTypesSupported(Iterable<? extends Symbol> keys) {
        for (Symbol key : keys) {
            DataType type = key.valueType();
            if (type instanceof CollectionType || type.equals(DataTypes.UNDEFINED)) {
                throw new UnsupportedOperationException("Cannot GROUP BY type: " + type);
            }
        }
    }

    @Override
    public BatchIterator<Row> apply(BatchIterator<Row> batchIterator) {
        return CollectingBatchIterator.newInstance(batchIterator, collector);
    }

    public Collector<Row, ?, Iterable<Row>> getCollector() {
        return collector;
    }

    @Override
    public boolean providesIndependentScroll() {
        return true;
    }
}
