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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedTableRelation;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationNormalizer;
import io.crate.analyze.symbol.FuncSymbols;
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
import io.crate.operation.operator.OrOperator;
import io.crate.operation.operator.any.AnyNeqOperator;
import io.crate.operation.operator.any.AnyOperator;
import io.crate.operation.predicate.NotPredicate;
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

import static io.crate.analyze.expressions.ExpressionAnalyzer.castIfNeededOrFail;
import static io.crate.operation.operator.Operators.LOGICAL_OPERATORS;
import static io.crate.operation.scalar.cast.CastFunctionResolver.isCastFunction;

final class SemiJoins {

    private final RelationNormalizer relationNormalizer;

    public SemiJoins(Functions functions) {
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
        List<Function> rewriteCandidates = gatherRewriteCandidates(where.query());
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
        for (Function rewriteCandidate : rewriteCandidates) {
            SelectSymbol selectSymbol = getSubqueryOrNull(rewriteCandidate.arguments().get(1));
            assert selectSymbol != null : "rewriteCandidate must contain a selectSymbol";

            // Avoid name clashes if the subquery is on the same relation; e.g.: select * from t1 where x in (select * from t1)
            QualifiedName subQueryName = selectSymbol.relation().getQualifiedName().withPrefix("S" + count);
            count++;
            Symbol joinCondition = makeJoinCondition(rewriteCandidate, sourceRel, selectSymbol.relation());
            semiJoinPairs.add(JoinPair.of(
                rel.getQualifiedName(),
                subQueryName,
                JoinType.SEMI,
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

    private static void removeRewriteCandidatesFromWhere(QueriedRelation rel, Collection<Function> rewriteCandidates) {
        rel.querySpec().where().replace(FuncSymbols.mapNodes(f -> {
            if (rewriteCandidates.contains(f)) {
                return Literal.BOOLEAN_TRUE;
            }
            return f;
        }));
    }

    static List<Function> gatherRewriteCandidates(Symbol query) {
        ArrayList<Function> candidates = new ArrayList<>();
        RewriteCandidateGatherer.INSTANCE.process(query, candidates);
        return candidates;
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
    static Symbol makeJoinCondition(Function rewriteCandidate, AnalyzedRelation sourceRel, QueriedRelation subRel) {
        assert getSubqueryOrNull(rewriteCandidate.arguments().get(1)).relation() == subRel
            : "subRel argument must match selectSymbol relation";

        rewriteCandidate = RefReplacer.replaceRefs(
            rewriteCandidate,
            r -> sourceRel.getField(r.ident().columnIdent(), Operation.READ));
        String name = rewriteCandidate.info().ident().name();
        assert name.startsWith(AnyOperator.OPERATOR_PREFIX) : "Can only create a join condition from any_";

        List<Symbol> args = rewriteCandidate.arguments();
        Symbol firstArg = args.get(0);
        List<DataType> newArgTypes = ImmutableList.of(firstArg.valueType(), firstArg.valueType());

        FunctionIdent joinCondIdent = new FunctionIdent(AnyOperator.nameToNonAny(name), newArgTypes);
        return new Function(
            new FunctionInfo(joinCondIdent, DataTypes.BOOLEAN),
            ImmutableList.of(firstArg, castIfNeededOrFail(subRel.fields().get(0), firstArg.valueType()))
        );
    }

    private static class RewriteCandidateGatherer extends SymbolVisitor<List<Function>, Boolean> {

        static final RewriteCandidateGatherer INSTANCE = new RewriteCandidateGatherer();

        @Override
        protected Boolean visitSymbol(Symbol symbol, List<Function> context) {
            return true;
        }

        @Override
        public Boolean visitFunction(Function func, List<Function> candidates) {
            String funcName = func.info().ident().name();

            /* Cannot rewrite a `op ANY subquery` expression into a semi-join if it's beneath a OR because
             * `op ANY subquery` has different semantics in case of NULL values than a semi-join would have
             */
            if (funcName.equals(OrOperator.NAME) || funcName.equals(NotPredicate.NAME) || funcName.equals(AnyNeqOperator.NAME)) {
                candidates.clear();
                return false;
            }

            if (LOGICAL_OPERATORS.contains(funcName)) {
                for (Symbol arg : func.arguments()) {
                    Boolean continueTraversal = process(arg, candidates);
                    if (!continueTraversal) {
                        return false;
                    }
                }
                return true;
            }

            if (funcName.startsWith(AnyOperator.OPERATOR_PREFIX)) {
                maybeAddSubQueryAsCandidate(func, candidates);
            }
            return true;
        }

        private static void maybeAddSubQueryAsCandidate(Function func, List<Function> candidates) {
            SelectSymbol subQuery = getSubqueryOrNull(func.arguments().get(1));
            if (subQuery == null) {
                return;
            }
            if (subQuery.getResultType() == SelectSymbol.ResultType.SINGLE_COLUMN_MULTIPLE_VALUES) {
                candidates.add(func);
            }
        }
    }
}
