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

package io.crate.planner.optimizer.rule;

import io.crate.analyze.WhereClause;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.where.DocKeys;
import io.crate.expression.eval.EvaluatingNormalizer;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.NodeContext;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.planner.WhereClauseOptimizer;
import io.crate.planner.operators.Collect;
import io.crate.planner.operators.Get;
import io.crate.planner.operators.LogicalPlan;
import io.crate.planner.optimizer.Rule;
import io.crate.planner.optimizer.matcher.Captures;
import io.crate.planner.optimizer.matcher.Pattern;
import io.crate.statistics.TableStats;

import java.util.Optional;

import static io.crate.planner.optimizer.matcher.Pattern.typeOf;

public final class RewriteCollectToGet implements Rule<Collect> {

    private final Pattern<Collect> pattern;

    public RewriteCollectToGet() {
        this.pattern = typeOf(Collect.class)
            .with(collect ->
                      collect.relation() instanceof DocTableRelation
                      && collect.where().hasQuery()
                      && !Symbols.containsColumn(collect.outputs(), DocSysColumns.FETCHID)
            );
    }

    @Override
    public Pattern<Collect> pattern() {
        return pattern;
    }

    @Override
    public LogicalPlan apply(Collect collect,
                             Captures captures,
                             TableStats tableStats,
                             TransactionContext txnCtx,
                             NodeContext nodeCtx) {
        var relation = (DocTableRelation) collect.relation();
        var normalizer = new EvaluatingNormalizer(nodeCtx, RowGranularity.CLUSTER, null, relation);
        WhereClause where = collect.where();
        var detailedQuery = WhereClauseOptimizer.optimize(
            normalizer,
            where.queryOrFallback(),
            relation.tableInfo(),
            txnCtx,
            nodeCtx
        );
        Optional<DocKeys> docKeys = detailedQuery.docKeys();
        //noinspection OptionalIsPresent no capturing lambda allocation
        if (docKeys.isPresent()) {
            return new Get(
                relation,
                docKeys.get(),
                detailedQuery.query(),
                collect.outputs(),
                tableStats.estimatedSizePerRow(relation.relationName())
            );
        } else {
            return null;
        }
    }
}
