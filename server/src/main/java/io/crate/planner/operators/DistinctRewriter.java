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

package io.crate.planner.operators;

import static io.crate.analyze.expressions.ExpressionAnalyzer.allocateFunction;

import java.util.ArrayList;
import java.util.List;

import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.execution.engine.aggregation.impl.CollectSetAggregation;
import io.crate.expression.symbol.Function;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.WindowFunction;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.planner.PlannerContext;

public class DistinctRewriter {
    private final CoordinatorTxnCtx txnCtx;
    private final NodeContext nodeCtx;

    public DistinctRewriter(CoordinatorTxnCtx txnCtx, NodeContext nodeCtx) {
        this.txnCtx = txnCtx;
        this.nodeCtx = nodeCtx;
    }

    public List<Symbol> rewrite(List<Symbol> outputs) {
        return null;
    }

    public static Function makeCollectSetFunction(Function original, CoordinatorTxnCtx coordinatorTxnCtx, NodeContext nodeCtx) {
        var arguments = original.arguments();
        var filter = original.filter();
        ExpressionAnalysisContext context = null;
        Boolean ignoreNulls = (original instanceof WindowFunction wf) ? wf.ignoreNulls() : null;

        return allocateFunction(
            CollectSetAggregation.NAME,
            arguments,
            filter,
            context,
            coordinatorTxnCtx,
            nodeCtx
        );
//        // define the outer function which contains the inner function as argument.
//        String nodeName = "collection_" + name;
//        List<Symbol> outerArguments = List.of(collectSetFunction);
//        try {
//            return allocateBuiltinOrUdfFunction(
//                schema, nodeName, outerArguments, null, ignoreNulls, context, true, windowDefinition, coordinatorTxnCtx, nodeCtx);
//        } catch (UnsupportedOperationException ex) {
//            throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
//                "unknown function %s(DISTINCT %s)", name, arguments.get(0).valueType()), ex);
//        }

    }


    private static List<Symbol> wrapDistinctWithCollectionCount(PlannerContext plannerContext, List<Symbol> outputs) {
        List<Symbol> list = new ArrayList<>();
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            if (output instanceof Function original && original.distinct()) {
                Symbol collectionCountWrap = wrapWithCollectionCount(plannerContext, original);
                list.add(collectionCountWrap);
            } else {
                list.add(output);
            }
        }
        return list;
    }
}
