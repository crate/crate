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
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;

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

    private static class NormalizerVisitor extends AnalyzedRelationVisitor<CoordinatorTxnCtx, AnalyzedRelation> {

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
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation<?> relation, CoordinatorTxnCtx context) {
            AnalyzedRelation normalizedSubRelation = process(relation.subRelation(), context);
            final QueriedSelectRelation<?> newRelation;
            if (normalizedSubRelation instanceof FieldResolver) {
                EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                    functions, RowGranularity.CLUSTER, null, ((FieldResolver) normalizedSubRelation));
                newRelation = relation
                    .replaceSubRelation(normalizedSubRelation)
                    .map(s -> evalNormalizer.normalize(s, context));
            } else {
                newRelation = relation.replaceSubRelation(normalizedSubRelation);
            }
            WhereClauseValidator.validate(newRelation.where().queryOrFallback());
            return newRelation;
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
        public AnalyzedRelation visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation,
                                                             CoordinatorTxnCtx context) {
            AnalyzedRelation analyzedRelation = process(relation.relation(), context);
            return new AliasedAnalyzedRelation(analyzedRelation, relation.getQualifiedName(), relation.columnAliases());
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, CoordinatorTxnCtx context) {
            return mss.mapSubRelations(
                rel -> rel.accept(this, context),
                s -> normalizer.normalize(s, context)
            );
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
