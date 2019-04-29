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

package io.crate.analyze.relations;

import io.crate.analyze.MultiSourceSelect;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.QueriedTable;
import io.crate.analyze.QuerySpec;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.Operation;

import static com.google.common.collect.Lists.transform;

/**
 * The RelationNormalizer tries to merge the tree of relations in a QueriedSelectRelation into a single AnalyzedRelation.
 * The merge occurs from the top level to the deepest one. For each level, it verifies if the query is mergeable with
 * the next relation and proceed with the merge if positive. When it is not, the partially merged tree is returned.
 */
public final class RelationNormalizer {

    private final NormalizerVisitor visitor;

    public RelationNormalizer(Functions functions) {
        visitor = new NormalizerVisitor(functions);
    }

    public AnalyzedRelation normalize(AnalyzedRelation relation, CoordinatorTxnCtx coordinatorTxnCtx) {
        return visitor.process(relation, coordinatorTxnCtx);
    }

    private class NormalizerVisitor extends AnalyzedRelationVisitor<CoordinatorTxnCtx, AnalyzedRelation> {

        private final Functions functions;
        private final EvaluatingNormalizer normalizer;

        NormalizerVisitor(Functions functions) {
            this.functions = functions;
            this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, CoordinatorTxnCtx context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, CoordinatorTxnCtx context) {
            AnalyzedRelation subRelation = relation.subRelation();
            AnalyzedRelation normalizedSubRelation = process(relation.subRelation(), context);
            if (subRelation == normalizedSubRelation) {
                return relation;
            }
            return new QueriedSelectRelation(
                relation.isDistinct(),
                normalizedSubRelation,
                transform(relation.fields(), Field::path),
                relation.querySpec().copyAndReplace(FieldReplacer.bind(f -> {
                    if (f.relation().equals(subRelation)) {
                        return normalizedSubRelation.getField(f.path(), Operation.READ);
                    }
                    return f;
                }))
            );
        }

        @Override
        public AnalyzedRelation visitView(AnalyzedView view, CoordinatorTxnCtx context) {
            AnalyzedRelation newSubRelation = process(view.relation(), context);
            if (newSubRelation == view.relation()) {
                return view;
            }
            return new AnalyzedView(view.name(), view.owner(), newSubRelation);
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable<?> queriedTable,
                                                  CoordinatorTxnCtx tnxCtx) {
            AbstractTableRelation<?> tableRelation = queriedTable.tableRelation();
            EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                functions, RowGranularity.CLUSTER, null, tableRelation);

            QueriedTable<? extends AbstractTableRelation<?>> table = new QueriedTable<>(
                queriedTable.isDistinct(),
                tableRelation,
                transform(queriedTable.fields(), Field::path),
                queriedTable.querySpec().copyAndReplace(s -> evalNormalizer.normalize(s, tnxCtx))
            );
            WhereClauseValidator.validate(table.where().queryOrFallback());
            return table;
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, CoordinatorTxnCtx context) {
            QuerySpec querySpec = mss.querySpec().copyAndReplace(s -> normalizer.normalize(s, context));

            // must create a new MultiSourceSelect because paths and query spec changed
            return MultiSourceSelect.createWithPushDown(RelationNormalizer.this, functions, context, mss, querySpec);
        }

        @Override
        public AnalyzedRelation visitOrderedLimitedRelation(OrderedLimitedRelation relation, CoordinatorTxnCtx context) {
            AnalyzedRelation childRelation = relation.childRelation();
            AnalyzedRelation normalizedChild = process(childRelation, context);
            if (normalizedChild == childRelation) {
                return relation;
            } else {
                return relation.map(normalizedChild, FieldReplacer.bind(f -> {
                    if (f.relation().equals(childRelation)) {
                        return normalizedChild.getField(f.path(), Operation.READ);
                    }
                    return f;
                }));
            }
        }

        @Override
        public AnalyzedRelation visitUnionSelect(UnionSelect unionSelect, CoordinatorTxnCtx context) {
            AnalyzedRelation left = process(unionSelect.left(), context);
            AnalyzedRelation right = process(unionSelect.right(), context);
            if (left == unionSelect.left() && right == unionSelect.right()) {
                return unionSelect;
            } else {
                return new UnionSelect(left, right);
            }
        }
    }
}
