package io.crate.planner.optimizer.rule;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;
import static io.crate.planner.optimizer.matcher.Patterns.source;
import static io.crate.planner.optimizer.rule.FilterOnJoinsUtil.moveQueryBelowJoin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import io.crate.analyze.relations.QuerySplitter;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.planner.operators.Filter;
import io.crate.planner.operators.HashJoin;
import io.crate.planner.operators.JoinPlan;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.costs.PlanStats;
import io.crate.planner.optimizer.matcher.Capture;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.sql.tree.JoinType;

public class MoveFilterIntoCrossJoin implements Rule<Filter>  {

    private final Capture<JoinPlan> joinCapture;
    private final Pattern<Filter> pattern;

    public MoveFilterIntoCrossJoin() {
        this.joinCapture = new Capture<>();
        this.pattern = typeOf(Filter.class)
            .with(source(),
                typeOf(JoinPlan.class)
                    .capturedAs(joinCapture)
                    .with(join -> join.joinType() == JoinType.CROSS)
            );
    }

    @Override
    public Pattern<Filter> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Filter filter,
                             Captures captures,
                             PlanStats planStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx,
                             Function<LogicalPlan, LogicalPlan> resolvePlan) {
        var join = captures.get(joinCapture);
        var query = filter.query();
        var queryParts = QuerySplitter.split(query);
        var relationNames = new HashSet<RelationName>();
        for (var relationName : queryParts.keySet()) {
            relationNames.addAll(relationName);
        }
        if (new HashSet<>(join.getRelationNames()).containsAll(relationNames)) {
            return new JoinPlan(join.lhs(), join.rhs(), JoinType.INNER, query);
        }
    return null;
    }
}
