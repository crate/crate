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

package io.crate.planner.operators;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import io.crate.expression.tablefunctions.ValuesFunction;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

public class RewriteInsertFromSubQueryToInsertFromValues implements Rule<Insert> {

    private final Capture<TableFunction> capture;
    private final Pattern<Insert> pattern;

    public RewriteInsertFromSubQueryToInsertFromValues() {
        this.capture = new Capture<>();
        this.pattern = typeOf(Insert.class)
            .with(source(), typeOf(TableFunction.class).capturedAs(capture));
    }

    @Override
    public Pattern<Insert> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Insert plan,
                             Captures captures,
                             Rule.Context context) {
        TableFunction tableFunction = captures.get(this.capture);
        var relation = tableFunction.relation();
        if (relation.function().name().equals(ValuesFunction.NAME)) {
            return new InsertFromValues(tableFunction.relation(), plan.columnIndexWriterProjection());
        } else {
            return null;
        }
    }
}
