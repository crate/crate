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

package io.crate.planner.consumer;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.symbol.FuncReplacer;
import io.crate.analyze.symbol.Function;
import io.crate.analyze.symbol.Literal;
import io.crate.analyze.symbol.RefReplacer;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.SymbolVisitor;
import io.crate.metadata.FunctionIdent;
import io.crate.metadata.FunctionInfo;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.table.Operation;
import io.crate.execution.expression.operator.OrOperator;
import io.crate.execution.expression.operator.any.AnyNeqOperator;
import io.crate.execution.expression.operator.any.AnyOperator;
import io.crate.execution.expression.predicate.NotPredicate;
import io.crate.planner.node.dql.join.JoinType;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;

import static io.crate.analyze.expressions.ExpressionAnalyzer.cast;
import static io.crate.execution.expression.operator.Operators.LOGICAL_OPERATORS;
import static io.crate.execution.expression.scalar.cast.CastFunctionResolver.isCastFunction;

final class SemiJoins {

    private final RelationNormalizer relationNormalizer;

    SemiJoins(Functions functions) {
        relationNormalizer = new RelationNormalizer(functions);
    }

    /**
     * Try to rewrite a QueriedRelation into a SemiJoin:
     *
     * <pre>
     *
     *     select x from t1 where x in (select id from t2)
     *               |
     *               v
     *     select t1.x from t1 SEMI JOIN (select id from t2) t2 on t1.x = t2.id
     * </pre>
     *
     * (Note that it's not possible to write a SemiJoin directly using SQL. But the example above is semantically close.)
     *
     * This rewrite isn't always possible - certain conditions need to be met for the semi-join to have the same
     * semantics as the subquery.
     *
     * @return the rewritten relation or null if a rewrite wasn't possible.
     */
    @Nullable
    QueriedRelation tryRewrite(QueriedRelation rel, TransactionContext transactionCtx) {
        WhereClause where = rel.querySpec().where();
        if (!where.hasQuery()) {
            return null;
        }
        List<Candidate> rewriteCandidates = gatherRewriteCandidates(where.query());
        if (rewriteCandidates.isEmpty()) {
            return null;
        }
        AnalyzedRelation sourceRel = getSource(rel);
        if (sourceRel == null) {
            return null;
        }


        // Function to turn Ref(x) back into Field(rel, x); it's required for the MultiSourceSelect structure;
        // (a lot of logic that follows in the Planner after the rewrite is based on Fields)
        java.util.function.Function<? super Symbol, ? extends Symbol> refsToFields =
            RefReplacer.replaceRefs(r -> sourceRel.getField(r.ident().columnIdent(), Operation.READ));

        removeRewriteCandidatesFromWhere(rel, rewriteCandidates);
        QuerySpec newTopQS = rel.querySpec().copyAndReplace(refsToFields);

        // Using MSS instead of TwoTableJoin so that the "fetch-pushdown" logic in the Planner is also applied
        HashMap<QualifiedName, AnalyzedRelation> sources = new LinkedHashMap<>(2);
        sources.put(rel.getQualifiedName(), sourceRel);

        ArrayList<JoinPair> semiJoinPairs = new ArrayList<>();
        int count = 0;
        for (Candidate rewriteCandidate : rewriteCandidates) {
            SelectSymbol selectSymbol = rewriteCandidate.subQuery;

            // Avoid name clashes if the subquery is on the same relation; e.g.: select * from t1 where x in (select * from t1)
            QualifiedName subQueryName = selectSymbol.relation().getQualifiedName().withPrefix("S" + count);
            count++;
            Symbol joinCondition = makeJoinCondition(rewriteCandidate, sourceRel);
            semiJoinPairs.add(JoinPair.of(
                rel.getQualifiedName(),
                subQueryName,
                rewriteCandidate.joinType,
                joinCondition
            ));
            sources.put(subQueryName, selectSymbol.relation());
        }

        // normalize is done to rewrite  SELECT * from t1, t2 to SELECT * from (select ... t1) t1, (select ... t2) t2
        // because planner logic expects QueriedRelation in the sources
        MultiSourceSelect mss = new MultiSourceSelect(
            sources,
            rel.fields(),
            newTopQS,
            semiJoinPairs
        );
        return (QueriedRelation) relationNormalizer.normalize(mss, transactionCtx);
    }

    @Nullable
    private static AnalyzedRelation getSource(QueriedRelation rel) {
        if (rel instanceof QueriedTableRelation) {
            return ((QueriedTableRelation) rel).tableRelation();
        }
        // TODO: support other cases as well
        return null;
    }

    private static void removeRewriteCandidatesFromWhere(QueriedRelation rel, Collection<Candidate> rewriteCandidates) {
        rel.querySpec().where().replace(FuncReplacer.mapNodes(f -> {
            for (Candidate rewriteCandidate : rewriteCandidates) {
                if (rewriteCandidate.function.equals(f)) {
                    return Literal.BOOLEAN_TRUE;
                }
            }
            return f;
        }));
    }

    static List<Candidate> gatherRewriteCandidates(Symbol query) {
        GatherRewriteCandidatesContext context = new GatherRewriteCandidatesContext();
        RewriteCandidateGatherer.INSTANCE.process(query, context);
        return context.candidates;
    }

    @Nullable
    static SelectSymbol getSubqueryOrNull(Symbol symbol) {
        symbol = unwrapCast(symbol);
        if (symbol instanceof SelectSymbol) {
            return ((SelectSymbol) symbol);
        }
        return null;
    }

    private static Symbol unwrapCast(Symbol symbol) {
        while (isCastFunction(symbol)) {
            symbol = ((Function) symbol).arguments().get(0);
        }
        return symbol;
    }

    /**
     * t1.x IN (select y from t2)  --> SEMI JOIN t1 on t1.x = t2.y
     */
    static Symbol makeJoinCondition(Candidate rewriteCandidate, AnalyzedRelation sourceRel) {
        Function anyFunc = RefReplacer.replaceRefs(
            rewriteCandidate.getAnyOpFunction(),
            r -> sourceRel.getField(r.ident().columnIdent(), Operation.READ));
        String name = anyFunc.info().ident().name();
        assert name.startsWith(AnyOperator.OPERATOR_PREFIX) : "Can only create a join condition from any_";

        List<Symbol> args = anyFunc.arguments();
        Symbol firstArg = args.get(0);
        List<DataType> newArgTypes = ImmutableList.of(firstArg.valueType(), firstArg.valueType());

        FunctionIdent joinCondIdent = new FunctionIdent(AnyOperator.nameToNonAny(name), newArgTypes);
        return new Function(
            new FunctionInfo(joinCondIdent, DataTypes.BOOLEAN),
            ImmutableList.of(firstArg, cast(rewriteCandidate.subQuery.relation().fields().get(0), firstArg.valueType()))
        );
    }

    @VisibleForTesting
    abstract static class Candidate {

        final Function function;
        final SelectSymbol subQuery;
        final JoinType joinType;

        Candidate(Function function, SelectSymbol subQuery, JoinType joinType) {
            this.function = function;
            this.subQuery = subQuery;
            this.joinType = joinType;
        }

        protected Function getAnyOpFunction() {
            return function;
        }
    }

    @VisibleForTesting
    static class SemiJoinCandidate extends Candidate {

        SemiJoinCandidate(Function function, SelectSymbol subQuery) {
            super(function, subQuery, JoinType.SEMI);
        }
    }

    @VisibleForTesting
    static class AntiJoinCandidate extends Candidate {

        AntiJoinCandidate(Function function, SelectSymbol subQuery) {
            super(function, subQuery, JoinType.ANTI);
        }

        @Override
        protected Function getAnyOpFunction() {
            return (Function) function.arguments().get(0);
        }
    }

    static Candidate buildCandidate(Function func, SelectSymbol subQuery, Function notPredicate) {
        if (notPredicate != null) {
            return new AntiJoinCandidate(notPredicate, subQuery);
        } else {
            return new SemiJoinCandidate(func, subQuery);
        }
    }

    private static class GatherRewriteCandidatesContext {

        private Function notPredicate;
        private final List<Candidate> candidates;

        private GatherRewriteCandidatesContext() {
            candidates = new ArrayList<>();
        }
    }

    private static class RewriteCandidateGatherer extends SymbolVisitor<GatherRewriteCandidatesContext, Boolean> {

        static final RewriteCandidateGatherer INSTANCE = new RewriteCandidateGatherer();

        @Override
        protected Boolean visitSymbol(Symbol symbol, GatherRewriteCandidatesContext context) {
            return true;
        }

        @Override
        public Boolean visitFunction(Function func, GatherRewriteCandidatesContext context) {
            String funcName = func.info().ident().name();

            /* Cannot rewrite a `op ANY subquery` expression into a semi-join if it's beneath a OR because
             * `op ANY subquery` has different semantics in case of NULL values than a semi-join would have
             */
            if (funcName.equals(OrOperator.NAME) || funcName.equals(AnyNeqOperator.NAME)) {
                context.candidates.clear();
                context.notPredicate = null;
                return false;
            }

            if (funcName.equals(NotPredicate.NAME)) {
                context.notPredicate = func;
                Boolean continueTraversal = process(func.arguments().get(0), context);
                context.notPredicate = null;
                return continueTraversal;
            }

            if (LOGICAL_OPERATORS.contains(funcName)) {
                for (Symbol arg : func.arguments()) {
                    Boolean continueTraversal = process(arg, context);
                    if (!continueTraversal) {
                        context.notPredicate = null;
                        return false;
                    }
                }
                context.notPredicate = null;
                return true;
            }

            if (funcName.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                maybeAddSubQueryAsCandidate(func, context.notPredicate, context.candidates);
            }
            context.notPredicate = null;
            return true;
        }

        private static void maybeAddSubQueryAsCandidate(Function func,
                                                        Function notPredicate,
                                                        List<Candidate> candidates) {
            SelectSymbol subQuery = getSubqueryOrNull(func.arguments().get(1));
            if (subQuery == null) {
                return;
            }
            if (subQuery.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
                candidates.add(buildCandidate(func, subQuery, notPredicate));
            }
        }
    }
}
