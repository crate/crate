/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.execution.dsl.phases;

import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.tablefunctions.TableFunctionImplementation;
import io.crate.planner.distribution.DistributionInfo;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.UUID;

public class TableFunctionCollectPhase extends AbstractProjectionsPhase implements CollectPhase {

    private final TableFunctionImplementation<?> functionImplementation;
    private final List<Literal<?>> functionArguments;
    private final List<Symbol> outputs;
    private final Symbol where;
    private final String nodeId;

    private DistributionInfo distributionInfo = DistributionInfo.DEFAULT_BROADCAST;

    public TableFunctionCollectPhase(UUID jobId,
                                     int phaseId,
                                     String nodeId,
                                     TableFunctionImplementation<?> functionImplementation,
                                     List<Literal<?>> functionArguments,
                                     List<Symbol> outputs,
                                     Symbol where) {
        super(jobId, phaseId, "tableFunction", List.of());
        this.nodeId = nodeId;
        this.functionImplementation = functionImplementation;
        this.functionArguments = functionArguments;
        this.outputs = outputs;
        this.where = where;
        this.outputTypes = Symbols.typeView(outputs);
    }

    public Symbol where() {
        return where;
    }

    @Override
    public Type type() {
        return Type.TABLE_FUNCTION_COLLECT;
    }

    @Override
    public <C, R> R accept(ExecutionPhaseVisitor<C, R> visitor, C context) {
        return visitor.visitTableFunctionCollect(this, context);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // current table functions can be executed on the handler - no streaming required
        throw new UnsupportedOperationException("NYI");
    }

    public List<Literal<?>> functionArguments() {
        return functionArguments;
    }

    public TableFunctionImplementation<?> functionImplementation() {
        return functionImplementation;
    }

    @Override
    public DistributionInfo distributionInfo() {
        return distributionInfo;
    }

    @Override
    public void distributionInfo(DistributionInfo distributionInfo) {
        this.distributionInfo = distributionInfo;
    }

    @Override
    public String name() {
        return "tableFunction";
    }

    @Override
    public Collection<String> nodeIds() {
        return List.of(nodeId);
    }


    @Override
    public List<Symbol> toCollect() {
        return outputs;
    }
}
