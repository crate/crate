/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.engine.aggregation;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

import org.elasticsearch.Version;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import io.crate.data.Input;
import io.crate.data.Row1;
import io.crate.execution.engine.aggregation.impl.MinimumAggregation;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.RowCollectExpression;
import io.crate.expression.symbol.AggregateMode;
import io.crate.expression.symbol.Literal;
import io.crate.metadata.FunctionType;
import io.crate.metadata.Functions;
import io.crate.metadata.Scalar;
import io.crate.metadata.functions.Signature;
import io.crate.metadata.settings.session.SessionSettingRegistry;
import io.crate.testing.PlainRamAccounting;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class GroupingCollectorTest {

    @Test
    public void test_addNewEntry_accounts_for_shallow_size_of_states_array() {
        RowCollectExpression keyInput = new RowCollectExpression(0);
        List<Input<?>> keyInputs = Collections.singletonList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));
        MinimumAggregation min = (MinimumAggregation) functions.getQualified(
            Signature.builder(MinimumAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.LONG),
            DataTypes.LONG
        );

        PlainRamAccounting ramAccounting = new PlainRamAccounting(-1);;
        var collector = GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { min },
            new Input[][] { new Input[] { keyInput }},
            new Input[] { Literal.BOOLEAN_TRUE },
            ramAccounting,
            null, // memoryManager is unused
            Version.CURRENT,
            new DataType[] { DataTypes.LONG },
            keyInputs.get(0),
            DataTypes.LONG
        );

        ResizeAwareMap<Object, Object[]> statesByKey = GroupByMaps.wrapperForJDKMap(new HashMap<>());
        collector.addNewEntry(statesByKey, 1L);

        assertThat(ramAccounting.totalBytes()).isEqualTo(112L);
    }

    @Test
    public void test_reduce_accounts_for_shallow_size_of_states_array() {
        RowCollectExpression keyInput = new RowCollectExpression(0);
        List<Input<?>> keyInputs = Collections.singletonList(keyInput);
        CollectExpression[] collectExpressions = new CollectExpression[]{keyInput};

        Functions functions = Functions.load(Settings.EMPTY, new SessionSettingRegistry(Set.of()));
        MinimumAggregation min = (MinimumAggregation) functions.getQualified(
            Signature.builder(MinimumAggregation.NAME, FunctionType.AGGREGATE)
                .argumentTypes(DataTypes.LONG.getTypeSignature())
                .returnType(DataTypes.LONG.getTypeSignature())
                .features(Scalar.Feature.DETERMINISTIC)
                .build(),
            List.of(DataTypes.LONG),
            DataTypes.LONG
        );

        PlainRamAccounting ramAccounting = new PlainRamAccounting(-1);;
        var collector = GroupingCollector.singleKey(
            collectExpressions,
            AggregateMode.ITER_FINAL,
            new AggregationFunction[] { min },
            new Input[][] { new Input[] { keyInput }},
            new Input[] { Literal.BOOLEAN_TRUE },
            ramAccounting,
            null, // memoryManager is unused
            Version.CURRENT,
            new DataType[] { DataTypes.LONG },
            keyInputs.get(0),
            DataTypes.LONG
        );

        ResizeAwareMap<Object, Object[]> statesByKey = GroupByMaps.wrapperForJDKMap(new HashMap<>());
        collector.reduce(statesByKey, new Row1(1L));

        assertThat(ramAccounting.totalBytes()).isEqualTo(88L);
    }



}
