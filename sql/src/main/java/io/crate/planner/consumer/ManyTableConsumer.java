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

import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Sets;
import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.OrderBy;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.TwoTableJoin;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.PlannedAnalyzedRelation;
import io.crate.analyze.relations.RelationSplitter;
import io.crate.analyze.symbol.DefaultTraversalSymbolVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.RelationColumn;
import io.crate.analyze.symbol.Symbol;
import io.crate.exceptions.ValidationException;
import io.crate.metadata.ReplacingSymbolVisitor;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.*;

public class ManyTableConsumer implements Consumer {

    private final Visitor visitor;

    public ManyTableConsumer(ConsumingPlanner consumingPlanner) {
        this.visitor = new Visitor(consumingPlanner);
    }

    @Override
    public PlannedAnalyzedRelation consume(AnalyzedRelation relation, ConsumerContext context) {
        return visitor.process(relation, context);
    }

    private static Collection<QualifiedName> getOrderedRelationNames(MultiSourceSelect statement) {
        if (!statement.querySpec().orderBy().isPresent()) {
            return statement.sources().keySet();
        }
        Map<AnalyzedRelation, QualifiedName> relationToName = new IdentityHashMap<>();
        for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : statement.sources().entrySet()) {
            relationToName.put(entry.getValue().relation(), entry.getKey());
        }

        OrderBy orderBy = statement.querySpec().orderBy().get();
        Set<QualifiedName> orderByOrder = new LinkedHashSet<>(statement.sources().size());
        Set<QualifiedName> names = new HashSet<>();
        Set<AnalyzedRelation> relations = new HashSet<>();

        for (Symbol orderBySymbol : orderBy.orderBySymbols()) {
            names.clear();
            QualifiedNameCounter.INSTANCE.process(orderBySymbol, names);
            if (names.isEmpty()) {
                RelationSplitter.RelationCounter.INSTANCE.process(orderBySymbol, relations);
                for (AnalyzedRelation relation : relations) {
                    names.add(relationToName.get(relation));
                }
            }
            orderByOrder.addAll(names);
        }
        if (orderByOrder.isEmpty()) {
            return statement.sources().keySet();
        }
        if (orderByOrder.size() < statement.sources().size()) {
            orderByOrder.addAll(statement.sources().keySet());
        }
        return orderByOrder;
    }

    /**
     * build a TwoTableJoin tree.
     * E.g. given a MSS with 3 tables:
     * <code>
     *     select t1.a, t2.b, t3.c from t1, t2, t3
     * </code>
     *
     * a TwoTableJoin tree is built:
     *
     * </code>
     *      join(
     *          join(t1, t2),
     *          t3
     *      )
     * </code>
     *
     * Where:
     * <code>
     *      join(t1, t2)
     *      has:
     *       QS: [ RC(t1, 0), RC(t2, 0) ]
     *       t1: select a from t1
     *       t2: select b from t2
     * </code>
     *
     * and
     * <code>
     *      join(join(t1, t2), t3)
     * has:
     *      QS: [ RC(join(t1, t2), 0), RC(join(t1, t2), 1),  RC(t3, 0) ]
     *      join(t1, t2) -
     *      t3: select c from t3
     *
     * </code>
     */
    static TwoTableJoin buildTwoTableJoinTree(MultiSourceSelect mss) {
        Collection<QualifiedName> orderedRelationNames = getOrderedRelationNames(mss);
        Iterator<QualifiedName> it = orderedRelationNames.iterator();

        QualifiedName leftName = it.next();
        QuerySpec rootQuerySpec = mss.querySpec();
        MultiSourceSelect.Source leftSource = mss.sources().get(leftName);
        AnalyzedRelation leftRelation = leftSource.relation();
        QuerySpec leftQuerySpec = leftSource.querySpec();
        Optional<OrderBy> remainingOrderBy = mss.remainingOrderBy();

        QualifiedName rightName;
        MultiSourceSelect.Source rightSource;
        while (it.hasNext()) {
            rightName = it.next();
            rightSource = mss.sources().get(rightName);

            Predicate<Symbol> predicate = new SubSetOfQualifiedNamesPredicate(Sets.newHashSet(leftName, rightName));

            QuerySpec newQuerySpec = rootQuerySpec.subset(predicate);
            if (remainingOrderBy.isPresent()) {
                remainingOrderBy = Optional.fromNullable(remainingOrderBy.get().subset(predicate));
            }
            TwoTableJoin join = new TwoTableJoin(
                    newQuerySpec,
                    leftName,
                    new MultiSourceSelect.Source(leftRelation, leftQuerySpec),
                    rightName,
                    rightSource,
                    remainingOrderBy
            );

            assert leftQuerySpec != null;
            final RelationColumnReWriteCtx reWriteCtx = new RelationColumnReWriteCtx(join);
            Function<? super Symbol, Symbol> replaceFunction = new Function<Symbol, Symbol>() {
                @Nullable
                @Override
                public Symbol apply(@Nullable Symbol input) {
                    return RelationColumnReWriter.INSTANCE.process(input, reWriteCtx);
                }
            };

            /**
             * create a new query spec where all RelationColumn symbols with a QualifiedName
             * of {@link RelationColumnReWriteCtx#left} or {@link RelationColumnReWriteCtx#right}
             * are replaced with a RelationColumn with QualifiedName of {@link RelationColumnReWriteCtx#newName}
             */
            rootQuerySpec = rootQuerySpec.copyAndReplace(replaceFunction);
            leftQuerySpec = newQuerySpec.copyAndReplace(replaceFunction);
            leftRelation = join;
            leftName = join.name();
        }
        return (TwoTableJoin) leftRelation;
    }

    private static class Visitor extends RelationPlanningVisitor {

        private final ConsumingPlanner consumingPlanner;

        public Visitor(ConsumingPlanner consumingPlanner) {
            this.consumingPlanner = consumingPlanner;
        }

        @Override
        public PlannedAnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, ConsumerContext context) {
            if (isUnsupportedStatement(mss, context)) return null;
            if (mss.sources().size() > 2 && (mss.remainingOrderBy().isPresent() || mss.querySpec().where().hasQuery())) {
                context.validationException(new ValidationException(
                        "Joining more than 2 tables with a join condition is not possible"));
                return null;
            }
            // TODO: instead of splitting Query spec and creating new source it would be possible to add a shortcut if mss.size() == 2

            replaceFieldsWithRelationColumns(mss);
            return planSubRelation(context, buildTwoTableJoinTree(mss));
        }


        private static boolean isUnsupportedStatement(MultiSourceSelect statement, ConsumerContext context) {
            if (statement.querySpec().groupBy().isPresent()) {
                context.validationException(new ValidationException("GROUP BY on CROSS JOIN is not supported"));
                return true;
            }
            if (statement.querySpec().hasAggregates()) {
                context.validationException(new ValidationException("AGGREGATIONS on CROSS JOIN is not supported"));
                return true;
            }

            return false;
        }

        private PlannedAnalyzedRelation planSubRelation(ConsumerContext context, AnalyzedRelation relation) {
            if (context.isRoot()) {
                return consumingPlanner.plan(relation, context);
            }
            return context.plannerContext().planSubRelation(relation, context);
        }

    }

    static void replaceFieldsWithRelationColumns(MultiSourceSelect mss) {
        final FieldToRelationColumnCtx ctx = new FieldToRelationColumnCtx(mss);
        mss.querySpec().replace(new Function<Symbol, Symbol>() {
            @Nullable
            @Override
            public Symbol apply(@Nullable Symbol input) {
                return FieldToRelationColumnVisitor.INSTANCE.process(input, ctx);
            }
        });
    }

    private static class SubSetOfQualifiedNamesPredicate implements Predicate<Symbol> {
        private final Set<QualifiedName> qualifiedNames;
        private final HashSet<QualifiedName> foundNames;

        public SubSetOfQualifiedNamesPredicate(Set<QualifiedName> qualifiedNames) {
            this.qualifiedNames = qualifiedNames;
            foundNames = new HashSet<>();
        }

        @Override
        public boolean apply(@Nullable Symbol input) {
            if (input == null) {
                return false;
            }
            foundNames.clear();
            QualifiedNameCounter.INSTANCE.process(input, foundNames);
            return Sets.difference(foundNames, qualifiedNames).isEmpty();
        }
    }

    public static class QualifiedNameCounter extends DefaultTraversalSymbolVisitor<Set<QualifiedName>, Void> {
        public static final QualifiedNameCounter INSTANCE = new QualifiedNameCounter();

        @Override
        public Void visitRelationColumn(RelationColumn relationColumn, Set<QualifiedName> context) {
            context.add(relationColumn.relationName());
            return null;
        }
    }

    private static class RelationColumnReWriteCtx {
        private final QualifiedName newName;
        private final QualifiedName left;
        private final QualifiedName right;
        private final int rightOffset;

        public RelationColumnReWriteCtx(TwoTableJoin join) {
            this(join.name(), join.leftName(), join.rightName(), join.left().querySpec().outputs().size());
        }

        public RelationColumnReWriteCtx(QualifiedName newName, QualifiedName left, QualifiedName right, int rightOffset) {
            this.newName = newName;
            this.left = left;
            this.right = right;
            this.rightOffset = rightOffset;
        }
    }

    private static class RelationColumnReWriter extends ReplacingSymbolVisitor<RelationColumnReWriteCtx> {

        private static final RelationColumnReWriter INSTANCE = new RelationColumnReWriter(false);

        public RelationColumnReWriter(boolean inPlace) {
            super(inPlace);
        }

        @Override
        public Symbol visitRelationColumn(RelationColumn relationColumn, RelationColumnReWriteCtx context) {
            if (relationColumn.relationName().equals(context.left)) {
                return new RelationColumn(context.newName, relationColumn.index(), relationColumn.valueType());
            }
            if (relationColumn.relationName().equals(context.right)) {
                return new RelationColumn(context.newName, relationColumn.index() + context.rightOffset, relationColumn.valueType());
            }
            return super.visitRelationColumn(relationColumn, context);
        }
    }

    private static class FieldToRelationColumnCtx {
        private final Map<AnalyzedRelation, QualifiedName> relationToName;
        private final MultiSourceSelect mss;

        public FieldToRelationColumnCtx(MultiSourceSelect mss) {
            relationToName = new IdentityHashMap<>(mss.sources().size());
            for (Map.Entry<QualifiedName, MultiSourceSelect.Source> entry : mss.sources().entrySet()) {
                relationToName.put(entry.getValue().relation(), entry.getKey());
            }
            this.mss = mss;
        }
    }

    private static class FieldToRelationColumnVisitor extends ReplacingSymbolVisitor<FieldToRelationColumnCtx> {

        private static final FieldToRelationColumnVisitor INSTANCE = new FieldToRelationColumnVisitor(true);

        public FieldToRelationColumnVisitor(boolean inPlace) {
            super(inPlace);
        }

        @Override
        public Symbol visitField(Field field, FieldToRelationColumnCtx ctx) {
            QualifiedName qualifiedName = ctx.relationToName.get(field.relation());
            int idx = 0;
            for (Symbol symbol : ctx.mss.sources().get(qualifiedName).querySpec().outputs()) {
                if (symbol instanceof Field) {
                    if (((Field) symbol).path().equals(field.path())) {
                        return new RelationColumn(qualifiedName, idx, field.valueType());
                    }
                }
                idx++;
            }
            return field;
        }
    }
}
