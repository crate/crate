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
import io.crate.breaker.RamAccounting;
import io.crate.data.ArrayBucket;
import io.crate.data.Row;
import io.crate.execution.engine.aggregation.AggregationFunction;
import io.crate.execution.engine.collect.InputCollectExpression;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.memory.MemoryManager;
import io.crate.memory.OnHeapMemoryManager;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionName;
import io.crate.metadata.Functions;
import io.crate.metadata.SearchPath;
import io.crate.test.integration.CrateUnitTest;
import io.crate.types.DataType;
import org.elasticsearch.Version;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static io.crate.testing.TestingHelpers.getFunctions;

public abstract class AggregationTest extends CrateUnitTest {

    protected static final RamAccounting RAM_ACCOUNTING = RamAccounting.NO_ACCOUNTING;

    protected Functions functions;
    protected MemoryManager memoryManager;

    @Before
    public void prepare() throws Exception {
        functions = getFunctions();
        memoryManager = new OnHeapMemoryManager(RAM_ACCOUNTING::addBytes);
    }

    public Object executeAggregation(String name, DataType dataType, Object[][] data) throws Exception {
        if (dataType == null) {
            return executeAggregation(name, dataType, data, ImmutableList.of());
        } else {
            return executeAggregation(name, dataType, data, ImmutableList.of(dataType));
        }
    }

    public Object executeAggregation(String name, DataType dataType, Object[][] data, List<DataType> argumentTypes) throws Exception {
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
        AggregationFunction impl = (AggregationFunction) functions.getQualified(fi);
        if (impl == null) {
            impl = (AggregationFunction) functions.resolveBuiltInFunctionBySignature(
                new FunctionName(null, name),
                argumentTypes,
                SearchPath.pathWithPGCatalogAndDoc()
            );
        }
        List<Object> states = new ArrayList<>();
        Version minNodeVersion = randomBoolean()
            ? Version.CURRENT
            : Version.V_4_0_9;
        states.add(impl.newState(RAM_ACCOUNTING, Version.CURRENT, minNodeVersion, memoryManager));
        for (Row row : new ArrayBucket(data)) {
            for (InputCollectExpression input : inputs) {
                input.setNextRow(row);
            }
            if (randomIntBetween(1, 4) == 1) {
                states.add(impl.newState(RAM_ACCOUNTING, Version.CURRENT, minNodeVersion, memoryManager));
            }
            int idx = states.size() - 1;
            states.set(idx, impl.iterate(RAM_ACCOUNTING, memoryManager, states.get(idx), inputs));
        }
        Object state = states.get(0);
        for (int i = 1; i < states.size(); i++) {
            state = impl.reduce(RAM_ACCOUNTING, state, states.get(i));
        }
        return impl.terminatePartial(RAM_ACCOUNTING, state);
    }

    protected Symbol normalize(String functionName, Object value, DataType type) {
        return normalize(functionName, Literal.of(type, value));
    }

    protected Symbol normalize(String functionName, Symbol... args) {
        List<Symbol> arguments = Arrays.asList(args);
        AggregationFunction function =
            (AggregationFunction) functions.get(null, functionName, arguments, SearchPath.pathWithPGCatalogAndDoc());
        return function.normalizeSymbol(
            new Function(function.info(), function.signature(), arguments),
            new CoordinatorTxnCtx(SessionContext.systemSessionContext()));
    }
}
