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
import io.crate.analyze.Rewriter;
import io.crate.analyze.WhereClause;
import io.crate.analyze.where.WhereClauseValidator;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.FieldReplacer;
import io.crate.metadata.Functions;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.planner.WhereClauseOptimizer;

/**
 * The RelationNormalizer tries to merge the tree of relations in a QueriedSelectRelation into a single QueriedRelation.
 * The merge occurs from the top level to the deepest one. For each level, it verifies if the query is mergeable with
 * the next relation and proceed with the merge if positive. When it is not, the partially merged tree is returned.
 */
public final class RelationNormalizer {

    private final NormalizerVisitor visitor;

    public RelationNormalizer(Functions functions) {
        visitor =  new NormalizerVisitor(functions);
    }

    public AnalyzedRelation normalize(AnalyzedRelation relation, TransactionContext transactionContext) {
        return visitor.process(relation, transactionContext);
    }

    private class NormalizerVisitor extends AnalyzedRelationVisitor<TransactionContext, AnalyzedRelation> {

        private final Functions functions;
        private final EvaluatingNormalizer normalizer;

        NormalizerVisitor(Functions functions) {
            this.functions = functions;
            this.normalizer = EvaluatingNormalizer.functionOnlyNormalizer(functions);
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, TransactionContext context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedSelectRelation(QueriedSelectRelation relation, TransactionContext context) {
            QueriedRelation subRelation = relation.subRelation();
            QueriedRelation normalizedSubRelation = (QueriedRelation) process(relation.subRelation(), context);
            if (subRelation == normalizedSubRelation) {
                return relation;
            }
            return new QueriedSelectRelation(
                normalizedSubRelation,
                relation.fields(),
                relation.querySpec().copyAndReplace(FieldReplacer.bind(f -> {
                    if (f.relation().equals(subRelation)) {
                        return normalizedSubRelation.getField(f.path(), Operation.READ);
                    }
                    return f;
                }))
            );
        }

        @Override
        public AnalyzedRelation visitView(AnalyzedView view, TransactionContext context) {
            AnalyzedRelation newSubRelation = process(view.relation(), context);
            if (newSubRelation == view.relation()) {
                return view;
            }
            return new AnalyzedView(view.name(), view.owner(), (QueriedRelation) newSubRelation);
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, TransactionContext context) {
            EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                functions, RowGranularity.CLUSTER, null, table.tableRelation());
            return new QueriedTable(
                table.tableRelation(),
                table.fields(),
                table.querySpec().copyAndReplace(s -> evalNormalizer.normalize(s, context))
            );
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, TransactionContext tnxCtx) {
            DocTableRelation docTableRelation = table.tableRelation();
            EvaluatingNormalizer evalNormalizer = new EvaluatingNormalizer(
                functions, RowGranularity.CLUSTER, null, docTableRelation);

            DocTableInfo tableInfo = docTableRelation.tableInfo;
            QuerySpec normalizedQS = table.querySpec().copyAndReplace(s -> evalNormalizer.normalize(s, tnxCtx));
            WhereClause where = normalizedQS.where();
            if (where.hasQuery()) {
                WhereClauseOptimizer.DetailedQuery detailedQuery = WhereClauseOptimizer.optimize(
                    normalizer, where.query(), tableInfo, tnxCtx);
                WhereClauseValidator.validate(detailedQuery.query());
                where = new WhereClause(
                    detailedQuery.query(),
                    detailedQuery.docKeys().orElse(null),
                    where.partitions(),
                    detailedQuery.clusteredBy()
                );
            }
            return new QueriedDocTable(docTableRelation, table.fields(), normalizedQS.where(where));
        }

        @Override
        public AnalyzedRelation visitMultiSourceSelect(MultiSourceSelect mss, TransactionContext context) {
            QuerySpec querySpec = mss.querySpec().copyAndReplace(s -> normalizer.normalize(s, context));

            // must create a new MultiSourceSelect because paths and query spec changed
            mss = MultiSourceSelect.createWithPushDown(RelationNormalizer.this, functions, context, mss, querySpec);
            Rewriter.tryRewriteOuterToInnerJoin(normalizer, mss);
            return mss;
        }

        @Override
        public AnalyzedRelation visitOrderedLimitedRelation(OrderedLimitedRelation relation, TransactionContext context) {
            process(relation.childRelation(), context);
            return relation;
        }

        @Override
        public AnalyzedRelation visitUnionSelect(UnionSelect unionSelect, TransactionContext context) {
            unionSelect.left((QueriedRelation) process(unionSelect.left(), context));
            unionSelect.right((QueriedRelation) process(unionSelect.right(), context));
            return unionSelect;
        }
    }
}
