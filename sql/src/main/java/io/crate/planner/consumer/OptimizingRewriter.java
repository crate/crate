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

import io.crate.analyze.QueriedTable;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.QueriedDocTable;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.metadata.Functions;
import io.crate.metadata.TransactionContext;

final class OptimizingRewriter {

    private final Functions functions;

    OptimizingRewriter(Functions functions) {
        this.functions = functions;
    }

    /*
         * Return the relation as is or a re-written relation
         */
    public AnalyzedRelation optimize(AnalyzedRelation relation, TransactionContext transactionContext) {
        return new Visitor(new SemiJoins(functions), transactionContext).process(relation, null);
    }

    private static class Visitor extends AnalyzedRelationVisitor<Void, AnalyzedRelation> {

        private final SemiJoins semiJoins;
        private final TransactionContext transactionContext;

        public Visitor(SemiJoins semiJoins, TransactionContext transactionContext) {
            this.semiJoins = semiJoins;
            this.transactionContext = transactionContext;
        }

        @Override
        protected AnalyzedRelation visitAnalyzedRelation(AnalyzedRelation relation, Void context) {
            return relation;
        }

        @Override
        public AnalyzedRelation visitQueriedTable(QueriedTable table, Void context) {
            return maybeApplySemiJoinRewrite(table);
        }

        @Override
        public AnalyzedRelation visitQueriedDocTable(QueriedDocTable table, Void context) {
            return maybeApplySemiJoinRewrite(table);
        }

        private QueriedRelation maybeApplySemiJoinRewrite(QueriedRelation queriedRelation) {
            if (!transactionContext.sessionContext().getSemiJoinsRewriteEnabled()) {
                return queriedRelation;
            }
            QueriedRelation rewrite = semiJoins.tryRewrite(queriedRelation, transactionContext);
            if (rewrite == null) {
                return queriedRelation;
            }
            return rewrite;
        }
    }
}
