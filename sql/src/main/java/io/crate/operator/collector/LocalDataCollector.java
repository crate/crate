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

package io.crate.operator.collector;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.procedures.ObjectObjectProcedure;
import com.google.common.collect.ImmutableList;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceResolver;
import io.crate.operator.Input;
import io.crate.operator.RowCollector;
import io.crate.planner.plan.CollectNode;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import org.cratedb.sql.CrateException;
import org.elasticsearch.common.Preconditions;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;

import java.util.ArrayList;
import java.util.List;

public class LocalDataCollector implements RowCollector<Object[][]> {

    private ESLogger logger = Loggers.getLogger(getClass());

    private final CollectNode collectNode;
    private final ReferenceResolver resolver;

    private final List<Symbol> inputs;
    private final List<Symbol> outputs;
    private final ObjectObjectOpenHashMap<Symbol, ReferenceImplementation> implementationMap;
    private final List<Object[]> results = new ArrayList<>();

    public LocalDataCollector(ReferenceResolver resolver, CollectNode collectNode) {
        this.resolver = resolver;
        // TODO: add/support functions?

        this.collectNode = collectNode;
        this.inputs = collectNode.inputs() != null ? collectNode.inputs() : ImmutableList.<Symbol>of();
        this.outputs = collectNode.outputs() != null ? collectNode.outputs() : ImmutableList.<Symbol>of();
        this.implementationMap = new ObjectObjectOpenHashMap<>(this.inputs.size());

        if (logger.isTraceEnabled()) {
            logger.trace("inputs: {}", collectNode.inputs());
            logger.trace("outputs: {}", collectNode.outputs());
        }
        Preconditions.checkState(this.inputs.containsAll(this.outputs),
                "Output symbols must be in input symbols");
        for (Symbol symbol : this.inputs) {
            switch (symbol.symbolType()) {
                case REFERENCE:
                    ReferenceImplementation impl = this.resolver.getImplementation(((Reference)symbol).info().ident());
                    if (impl == null) {
                        throw new CrateException(String.format("Unknown Reference")); // TODO: better exception
                    }
                    this.implementationMap.put(symbol, impl);
                    break;
                default:
                    throw new CrateException(String.format("Symbol %s not supported", symbol.toString()));

            }
        }
    }

    @Override
    public boolean startCollect() {
        return this.outputs.size() > 0;
    }

    @Override
    public boolean processRow() {
        final Object[] rowResult = new Object[outputs.size()];
        this.implementationMap.forEach(new ObjectObjectProcedure<Symbol, ReferenceImplementation>() {

            @Override
            public void apply(Symbol key, ReferenceImplementation value) {
                if (value instanceof Input<?>) {
                    Object symbolValue = ((Input) value).value();
                    int outputIdx = outputs.indexOf(key);
                    assert outputIdx >= 0;
                    rowResult[outputIdx] = symbolValue;
                }
            }
        });
        results.add(rowResult);
        return false;
    }

    @Override
    public Object[][] finishCollect() {
        return results.toArray(new Object[results.size()][]);
    }
}
