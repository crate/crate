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

package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;

import org.elasticsearch.Version;

import io.crate.expression.symbol.Literal;
import io.crate.planner.operators.GroupHashAggregate;
import io.crate.planner.operators.Limit;
import io.crate.planner.operators.LimitDistinct;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.Stats;
import io.crate.types.DataTypes;


/**
 * A rule to rewrite a `SELECT DISTINCT [...] LIMIT n` to use a special LimitDistinct operator.
 *
 * <p>
 * The {@link LimitDistinct} operator support early termination, reducing the amount of keys that have to be
 * consumed/processed from the source.
 * </p>
 *
 * <p>
 * This optimization is more efficient the lower the limit and the higher the cardinality ratio. (E.g. a unique column).
 * On a ENUM style column that only has like 5 distinct values in a large table, this {@link LimitDistinct}
 * operator could cause a slow down:
 *
 * The "early terminate" case wouldn't happen; It would process almost all rows.
 * In that case our "optimized group by iterator" is more efficient as it contains a ordinal optimization.
 * (It already knows which values are unique and skips everything else)
 * </p>
 */
public final class RewriteGroupByKeysLimitToLimitDistinct implements Rule<Limit> {

    private final Pattern<Limit> pattern;
    private final Capture<GroupHashAggregate> groupCapture;

    public RewriteGroupByKeysLimitToLimitDistinct() {
        this.groupCapture = new Capture<>();
        this.pattern = typeOf(Limit.class)
            .with(
                source(),
                typeOf(GroupHashAggregate.class)
                    .capturedAs(groupCapture)
                    .with(groupAggregate -> groupAggregate.aggregates().isEmpty())
                );
    }

    private static boolean eagerTerminateIsLikely(Limit limit,
                                                  GroupHashAggregate groupAggregate,
                                                  PlanStats planStats) {
        if (groupAggregate.outputs().size() > 1 || !groupAggregate.outputs().get(0).valueType().equals(DataTypes.STRING)) {
            // `GroupByOptimizedIterator` can only be used for single text columns.
            // If that is not the case we can always use LimitDistinct even if a eagerTerminate isn't likely
            // because a regular GROUP BY would have to do at least the same amount of work in any case.
            return true;
        }
        Stats groupHashAggregateStats = planStats.get(groupAggregate);
        var limitSymbol = limit.limit();
        if (limitSymbol instanceof Literal) {
            var limitVal = DataTypes.INTEGER.sanitizeValue(((Literal<?>) limitSymbol).value());
            // Would consume all source rows -> prefer default group by implementation which has other optimizations
            // which are more beneficial in this scenario
            if (limitVal > groupHashAggregateStats.numDocs()) {
                return false;
            }
        }
        long sourceRows = planStats.get(groupAggregate.source()).numDocs();
        if (sourceRows == 0) {
            return false;
        }
        var cardinalityRatio = groupHashAggregateStats.numDocs() / sourceRows;
        /*
         * The threshold was chosen after comparing `with limitDistinct` vs. `without limitDistinct`
         *
         * create table ids_with_tags (id text primary key, tag text not null)
         *
         * Running:
         *      select distinct tag from ids_with_tags limit 5
         *      `ids_with_tags` containing 5000 rows
         *
         * Data having been generated using:
         *
         *  mkjson --num <num_unique_tags> tag="ulid()" | jq -r '.tag' >! tags.txt
         *  mkjson --num 5000 id="ulid()" tag="oneOf(fromFile('tags.txt'))" >! specs/data/ids_with_tag.json
         *
         *      Number of unique tags: 1
         *      median: +  50.91%
         *      median: +  46.94%
         *      Number of unique tags: 2
         *      median: +  10.30%
         *      median: +  69.29%
         *      Number of unique tags: 3
         *      median: +  51.35%
         *      median: -  15.93%
         *      Number of unique tags: 4
         *      median: +   4.35%
         *      median: +  18.13%
         *      Number of unique tags: 5
         *      median: - 157.07%
         *      median: - 120.55%
         *      Number of unique tags: 6
         *      median: - 129.60%
         *      median: - 150.84%
         *      Number of unique tags: 7
         *
         * With 500_000 rows:
         *
         *      Number of unique tags: 1
         *      median: +  98.69%
         *      median: +  81.54%
         *      Number of unique tags: 2
         *      median: +  73.22%
         *      median: +  82.40%
         *      Number of unique tags: 3
         *      median: + 124.86%
         *      median: + 107.27%
         *      Number of unique tags: 4
         *      median: + 102.73%
         *      median: +  69.48%
         *      Number of unique tags: 5
         *      median: - 199.17%
         *      median: - 199.36%
         *      Number of unique tags: 6
         *
         */
        double threshold = 0.001;
        return cardinalityRatio > threshold;
    }

    @Override
    public Pattern<Limit> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Limit limit,
                             Captures captures,
                             Rule.Context context) {
        GroupHashAggregate groupBy = captures.get(groupCapture);
        if (!eagerTerminateIsLikely(limit, groupBy, context.planStats())) {
            return null;
        }
        return new LimitDistinct(
            groupBy.source(),
            limit.limit(),
            limit.offset(),
            groupBy.outputs()
        );
    }

    @Override
    public Version requiredVersion() {
        return Version.V_4_1_0;
    }
}
