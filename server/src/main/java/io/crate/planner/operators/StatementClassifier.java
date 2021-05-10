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

import io.crate.planner.Plan;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * Utility to classify SQL statements based on their {@link Plan}.
 *
 * A classification consists of the {@link io.crate.planner.Plan.StatementType} that is provided by the {@link Plan}
 * implementation, such as DDL, SELECT, INSERT, UPDATE, DELETE, etc., and in case of a SELECT statement, additional
 * labels. These labels are a set of {@link LogicalPlan}s that the statement plan consists of, for example:
 *
 * <pre>
 *     SELECT * FROM users ORDER BY age DESC;
 * </pre>
 *
 * Would result in a classification of:
 *
 * <pre>
 *     type = SELECT
 *     labels = [Collect, Eval, Order]
 * </pre>
 */
public final class StatementClassifier extends LogicalPlanVisitor<Set<String>, Void> {

    private static final StatementClassifier INSTANCE = new StatementClassifier();

    private StatementClassifier() {
    }

    public static Classification classify(Plan plan) {
        if (plan instanceof LogicalPlan) {
            Set<String> classes = new TreeSet<>();
            ((LogicalPlan) plan).accept(INSTANCE, classes);
            return new Classification(plan.type(), classes);
        } else {
            return new Classification(plan.type());
        }
    }

    public static class Classification {

        private final Set<String> labels;
        private final Plan.StatementType type;

        public Classification(Plan.StatementType type, Set<String> labels) {
            this.type = type;
            this.labels = labels;
        }

        public Classification(Plan.StatementType type) {
            this.type = type;
            this.labels = Collections.emptySet();
        }

        public Set<String> labels() {
            return labels;
        }

        public Plan.StatementType type() {
            return type;
        }

        @Override
        public String toString() {
            return "Classification{type=" + type + ", labels=" + labels + "}";
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Classification that = (Classification) o;
            return Objects.equals(labels, that.labels) &&
                   type == that.type;
        }

        @Override
        public int hashCode() {
            return Objects.hash(labels, type);
        }
    }

    @Override
    protected Void visitPlan(LogicalPlan logicalPlan, Set<String> context) {
        for (var source : logicalPlan.sources()) {
            source.accept(this, context);
        }
        context.add(logicalPlan.getClass().getSimpleName());
        return null;
    }

    @Override
    public Void visitRootRelationBoundary(RootRelationBoundary logicalPlan, Set<String> context) {
        logicalPlan.source.accept(this, context);
        return null;
    }

    @Override
    public Void visitInsert(Insert logicalPlan, Set<String> context) {
        logicalPlan.source.accept(this, context);
        return null;
    }

    @Override
    public Void visitRename(Rename rename, Set<String> context) {
        // skip rename; not relevant for query metrics
        rename.source.accept(this, context);
        return null;
    }

    @Override
    public Void visitInsert(InsertFromValues logicalPlan, Set<String> context) {
        return visitPlan(logicalPlan, context);
    }

    @Override
    public Void visitMultiPhase(MultiPhase logicalPlan, Set<String> context) {
        for (LogicalPlan plan : logicalPlan.dependencies().keySet()) {
            plan.accept(this, context);
        }
        return visitPlan(logicalPlan, context);
    }
}
