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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.SqlFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.Query;

public final class CreateViewAnalyzer {

    private final RelationAnalyzer relationAnalyzer;

    CreateViewAnalyzer(RelationAnalyzer relationAnalyzer) {
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedStatement analyze(CreateView createView, CoordinatorTxnCtx txnCtx) {
        RelationName name = RelationName.of(createView.name(), txnCtx.sessionContext().searchPath().currentSchema());
        name.ensureValidForRelationCreation();
        if (BlobSchemaInfo.NAME.equals(name.schema())) {
            throw new UnsupportedOperationException("Creating a view in the \"blob\" schema is not supported");
        }
        AnalyzedRelation query;
        try {
            String formattedQuery = SqlFormatter.formatSql(createView.query());
            // Analyze the formatted Query to make sure the formatting didn't mess it up in any way.
            query = relationAnalyzer.analyzeUnbound((Query) SqlParser.createStatement(formattedQuery), txnCtx, ParamTypeHints.EMPTY);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Query cannot be used in a VIEW: " + createView.query());
        }

        // We do not bother with exists checks here because it wouldn't be "atomic" as it might be based
        // on an outdated cluster check, leading to a potential race condition.
        // The "masterOperation" which will update the clusterState will do a real-time verification

        if (query.fields().stream().map(f -> f.path().sqlFqn()).distinct().count() != query.fields().size()) {
            throw new IllegalArgumentException("Query in CREATE VIEW must not have duplicate column names");
        }
        return new CreateViewStmt(
            name,
            query,
            createView.query(),
            createView.replaceExisting(),
            txnCtx.sessionContext().user()
        );
    }
}
