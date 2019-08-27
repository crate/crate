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

import io.crate.planner.operators.Eval;
import io.crate.planner.operators.Fetch;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;

import javax.annotation.Nullable;
import java.util.List;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

/**
 * <p>
 * We want to reduce column/value look-ups as much as possible as those are fairly expensive.
 * One way to accomplish that is to defer the value look-ups until after the data has been limited or filtered.
 * </p>
 *
 * In a query `SELECT * FROM t LIMIT 100` with a 5 node cluster. A simple execution would:
 *
 * <ul>
 *   <li>Collect 100 rows with all columns from each node</li>
 *   <li>Merge the result, up to 100 rows (the limit) (throwing away 400 rows)</li>
 * </ul>
 *
 * The reason we need to do this (selecting 100 rows from EACH node), is that we don't know:
 *
 * <ul>
 *  <li>How many rows match a query on a given node</li>
 *  <li>How many rows there are on a given node</li>
 * </ul>
 *
 * Thus, we can't say "fetch 40 records from A and another 60 from B), but what we can do is:
 * <ul>
 *   <li>Collect 100 rows from each node, but only collect a {@link io.crate.execution.engine.fetch.FetchId} as a place-holder for all columns of a row.</li>
 *   <li>Merge the result, up to 100 rows (the limit)</li>
 *   <li>Retrieve the columns for those 100 rows, utilizing the earlier collected FetchId.</li>
 * </ul>
 *
 * Other scenarios where we can apply this optimization is for example for JOINs which can also reduce the data set,
 * and where only a sub-set of the columns is required to evaluate the join condition.
 * We can fetch the remaining columns later on in the same fashion as described further above.
 */
public class QueryThenFetch implements Rule<Eval> {

    private final Pattern<Eval> pattern;

    public QueryThenFetch() {
        pattern = typeOf(Eval.class);
    }

    private boolean hasResultReducingOperatorAsSource(Eval eval) {
        for (LogicalPlan source : eval.sources()) {
            // TODO: check that these are actually reducing rows.
            // e.g. on a table that has 1000 rows, a limit of 10000 shouldn't cause a QTF(?)
            // TODO: Add NL with joinCondition
            if (source instanceof Limit || source instanceof HashJoin || source instanceof Filter) {
                return true;
            }
        }
        return false;
    }

    @Override
    public Pattern<Eval> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Eval eval, Captures captures) {
        if (!hasResultReducingOperatorAsSource(eval)) {
            return tryRemoveRedundantEval(eval);
        }
        var pruneResult = eval.source().rewriteForQueryThenFetch(List.of());
        if (pruneResult.supportsFetch()) {
            LogicalPlan prunedSource = pruneResult.createRewrittenOperator();
            return new Fetch(prunedSource, eval.outputs());
        } else {
            return tryRemoveRedundantEval(eval);
        }
    }

    @Nullable
    private static LogicalPlan tryRemoveRedundantEval(Eval eval) {
        if (eval.outputs().equals(eval.source().outputs())) {
            return eval.source();
        } else {
            return null;
        }
    }
}
