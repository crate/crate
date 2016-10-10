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

package io.crate.planner;

import io.crate.analyze.*;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.ParameterSymbol;
import io.crate.analyze.symbol.SelectSymbol;
import io.crate.analyze.symbol.Symbol;
import io.crate.metadata.Functions;
import io.crate.metadata.ReplaceMode;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.planner.consumer.ConsumingPlanner;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Singleton
public class MultiPhasePlanner {

    private final RelationVisitor relationVisitor;
    private final SubquerySymbolVisitor subquerySymbolVisitor;
    private final ClusterService clusterService;
    private final Functions functions;
    private final ConsumingPlanner consumingPlanner;
    private final Planner planner;
    private final StatementVisitor statementVisitor;
    private final EvaluatingNormalizer normalizer;

    @Inject
    public MultiPhasePlanner(ClusterService clusterService,
                             Functions functions,
                             ConsumingPlanner consumingPlanner,
                             Planner planner) {
        this.clusterService = clusterService;
        this.functions = functions;
        this.consumingPlanner = consumingPlanner;
        this.planner = planner;
        relationVisitor = new RelationVisitor();
        subquerySymbolVisitor = new SubquerySymbolVisitor();
        statementVisitor = new StatementVisitor();
        normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions, ReplaceMode.COPY);
    }

    public Plan plan(Analysis analysis, UUID jobId, int softLimit, int fetchSize) {
        /*
        AnalyzedStatement analyzedStatement = analysis.analyzedStatement();
        Context context = new Context(new Planner.Context(
            clusterService, jobId, consumingPlanner, normalizer, analysis.transactionContext(), softLimit, fetchSize));
        statementVisitor.process(analyzedStatement, context);
        */
        return planner.plan(analysis, jobId, softLimit, fetchSize);
    }

    private static class Node {
        private final List<Plan> plans = new ArrayList<>();
        private Node next;
    }

    private static class Context {

        private final Planner.Context plannerContext;
        private final List<Node> nodes = new ArrayList<>();
        private final Node rootNode = new Node();

        private int placeHolderIdx = 0;
        private int level = 0;

        public Context(Planner.Context plannerContext) {
            this.plannerContext = plannerContext;
            nodes.add(rootNode);
        }

        void addPlan(Plan plan) {
            nodes.get(level).plans.add(plan);
        }

        int newPlaceholder() {
            return placeHolderIdx++;
        }

        void incLevel() {
            Node prevNode = nodes.get(level);
            level++;
            Node currentNode;
            if (nodes.size() <= level) {
                currentNode = new Node();
                prevNode.next = currentNode;
                nodes.add(currentNode);
            }
        }

        void decLevel() {
            level--;
        }
    }

    private class StatementVisitor extends AnalyzedStatementVisitor<Context, Void> {
        @Override
        protected Void visitSelectStatement(SelectAnalyzedStatement statement, Context context) {
            return relationVisitor.process(statement.relation(), context);
        }
    }

    private class RelationVisitor extends AnalyzedRelationVisitor<Context, Void> {

        @Override
        public Void visitQueriedTable(QueriedTable table, Context context) {
            WhereClause where = table.querySpec().where();
            if (where.hasQuery()) {
                subquerySymbolVisitor.process(where.query(), context);
            }
            return null;
        }
    }

    private class SubquerySymbolVisitor extends ReplacingSymbolVisitor<Context> {

        SubquerySymbolVisitor() {
            super(ReplaceMode.MUTATE);
        }

        @Override
        public Symbol visitSelectSymbol(SelectSymbol selectSymbol, Context context) {
            AnalyzedRelation relation = selectSymbol.relation();
            context.incLevel();
            relationVisitor.process(relation, context);
            SelectAnalyzedStatement statement = new SelectAnalyzedStatement((QueriedRelation ) relation);
            Plan plan = planner.process(statement, context.plannerContext);
            // TODO: track plan <-> placeHolder relation
            context.addPlan(plan);
            context.decLevel();
            return new ParameterSymbol(context.newPlaceholder(), selectSymbol.valueType());
        }
    }
}
