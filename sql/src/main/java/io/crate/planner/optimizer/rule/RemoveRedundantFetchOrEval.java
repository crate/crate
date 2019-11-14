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

package io.crate.planner.optimizer.rule;

import io.crate.metadata.TransactionContext;
import io.crate.statistics.TableStats;
import io.crate.planner.operators.FetchOrEval;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

/**
 * Eliminates any Eval nodes that have the same output as their source
 */
public final class RemoveRedundantFetchOrEval implements Rule<FetchOrEval> {

    private final Pattern<FetchOrEval> pattern;

    public RemoveRedundantFetchOrEval() {
        this.pattern = typeOf(FetchOrEval.class)
            .with(fetchOrFetchOrEval -> fetchOrFetchOrEval.outputs().equals(fetchOrFetchOrEval.source().outputs()));
    }

    @Override
    public Pattern<FetchOrEval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(FetchOrEval plan,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx) {
        return plan.source();
    }
}
