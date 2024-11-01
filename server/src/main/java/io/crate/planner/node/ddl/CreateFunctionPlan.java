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

package io.crate.planner.node.ddl;

import java.util.function.Function;

import org.elasticsearch.action.support.master.AcknowledgedResponse;

import io.crate.analyze.AnalyzedCreateFunction;
import io.crate.analyze.SymbolEvaluator;
import io.crate.data.Row;
import io.crate.data.Row1;
import io.crate.data.RowConsumer;
import io.crate.execution.support.OneRowActionListener;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.udf.CreateUserDefinedFunctionRequest;
import io.crate.expression.udf.UserDefinedFunctionMetadata;
import io.crate.planner.DependencyCarrier;
import io.crate.planner.Plan;
import io.crate.planner.PlannerContext;
import io.crate.planner.operators.SubQueryResults;
import io.crate.types.StringType;

public class CreateFunctionPlan implements Plan {

    private final AnalyzedCreateFunction createFunction;

    public CreateFunctionPlan(AnalyzedCreateFunction createFunction) {
        this.createFunction = createFunction;
    }

    @Override
    public StatementType type() {
        return StatementType.DDL;
    }

    @Override
    public void executeOrFail(DependencyCarrier dependencies,
                              PlannerContext plannerContext,
                              RowConsumer consumer,
                              Row params,
                              SubQueryResults subQueryResults) throws Exception {
        Function<? super Symbol, Object> eval = x -> SymbolEvaluator.evaluate(
            plannerContext.transactionContext(),
            plannerContext.nodeContext(),
            x,
            params,
            subQueryResults
        );

        UserDefinedFunctionMetadata metadata = new UserDefinedFunctionMetadata(
            createFunction.schema(),
            createFunction.name(),
            createFunction.arguments(),
            createFunction.returnType(),
            StringType.INSTANCE.sanitizeValue(eval.apply(createFunction.language())),
            StringType.INSTANCE.sanitizeValue(eval.apply(createFunction.definition()))
        );
        CreateUserDefinedFunctionRequest request = new CreateUserDefinedFunctionRequest(metadata, createFunction.replace());
        OneRowActionListener<AcknowledgedResponse> listener = new OneRowActionListener<>(consumer, ignoredResponse -> new Row1(1L));
        dependencies.createFunctionAction().execute(request).whenComplete(listener);
    }
}
