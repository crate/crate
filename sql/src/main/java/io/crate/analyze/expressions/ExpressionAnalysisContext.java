/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.expressions;

import io.crate.analyze.EvaluatingNormalizer;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.FunctionInfo;
import io.crate.operation.scalar.timestamp.CurrentTimestampFunction;
import io.crate.sql.tree.CurrentTime;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ExpressionAnalysisContext {

    /**
     * currentTime should be evaluated only once per query/statement.
     * This map is used to
     */
    private final Map<CurrentTime, Literal> allocatedCurrentTimestamps = new HashMap<>();

    public boolean hasAggregates = false;

    public ExpressionAnalysisContext() {
    }

    public Function allocateFunction(FunctionInfo functionInfo, List<Symbol> arguments) {
        Function newFunction = new Function(functionInfo, arguments);
        hasAggregates = hasAggregates || functionInfo.type() == FunctionInfo.Type.AGGREGATE;
        return newFunction;
    }

    /**
     * allocate a new function for the currentTime node or re-use an already evaluated one to ensure
     * currentTime evaluation is global per query.
     *
     * TODO: once sub-selects are implemented need to take care to re-use the ExpressionAnalysisContext...
     */
    public Symbol allocateCurrentTime(CurrentTime node, List<Symbol> args, EvaluatingNormalizer normalizer) {
        Literal literal = allocatedCurrentTimestamps.get(node);
        if (literal == null) {
            literal = (Literal)normalizer.normalize(allocateFunction(CurrentTimestampFunction.INFO, args));
            allocatedCurrentTimestamps.put(node, literal);
        }
        return literal;
    }
}
