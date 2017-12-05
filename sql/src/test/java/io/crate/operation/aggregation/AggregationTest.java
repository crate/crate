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

package io.crate.operation.aggregation;

import com.google.common.collect.ImmutableList;
import io.crate.action.sql.SessionContext;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.breaker.RamAccountingContext;
import io.crate.data.ArrayBucket;
import io.crate.data.Row;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.operation.collect.InputCollectExpression;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.getFunctions;

public abstract class AggregationTest extends CrateUnitTest {

    protected static final RamAccountingContext ramAccountingContext =
        new RamAccountingContext("dummy", new NoopCircuitBreaker(CircuitBreaker.FIELDDATA));

    protected Functions functions;

    @Before
    public void prepare() throws Exception {
        functions = getFunctions();
    }

    public Object[][] executeAggregation(String name, DataType dataType, Object[][] data) throws Exception {
        if (dataType == null) {
            return executeAggregation(name, dataType, data, ImmutableList.of());
        } else {
            return executeAggregation(name, dataType, data, ImmutableList.of(dataType));
        }
    }

    public Object[][] executeAggregation(String name, DataType dataType, Object[][] data, List<DataType> argumentTypes) throws Exception {
        FunctionIdent fi;
        InputCollectExpression[] inputs;
        if (dataType != null) {
            fi = new FunctionIdent(name, argumentTypes);
            inputs = new InputCollectExpression[argumentTypes.size()];
            for (int i = 0; i < argumentTypes.size(); i++) {
                inputs[i] = new InputCollectExpression(i);
            }
        } else {
            fi = new FunctionIdent(name, ImmutableList.of());
            inputs = new InputCollectExpression[0];
        }
        AggregationFunction impl = (AggregationFunction) functions.getBuiltin(fi.name(), fi.argumentTypes());
        Object state = impl.newState(ramAccountingContext, Version.CURRENT, BigArrays.NON_RECYCLING_INSTANCE);

        ArrayBucket bucket = new ArrayBucket(data);

        for (Row row : bucket) {
            for (InputCollectExpression i : inputs) {
                i.setNextRow(row);
            }
            state = impl.iterate(ramAccountingContext, state, inputs);

        }
        state = impl.terminatePartial(ramAccountingContext, state);
        return new Object[][]{{state}};
    }

    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        DataType[] argTypes = new DataType[args.length];
        for (int i = 0; i < args.length; i++) {
            argTypes[i] = args[i].valueType();
        }
        AggregationFunction function =
            (AggregationFunction) functions.getBuiltin(functionName, Arrays.asList(argTypes));
        return function.normalizeSymbol(new Function(function.info(), Arrays.asList(args)), new TransactionContext(SessionContext.create()));
    }
}
