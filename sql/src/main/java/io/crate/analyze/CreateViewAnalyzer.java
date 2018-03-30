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

import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.expression.symbol.Field;
import io.crate.expression.symbol.format.SymbolPrinter;
import io.crate.metadata.Functions;
import io.crate.metadata.RelationName;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.Query;

public final class CreateViewAnalyzer {

    private final RelationAnalyzer relationAnalyzer;
    private final SQLPrinter sqlPrinter;

    CreateViewAnalyzer(Functions functions, RelationAnalyzer relationAnalyzer) {
        this.sqlPrinter = new SQLPrinter(new SymbolPrinter(functions));
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedStatement analyze(CreateView createView, TransactionContext txnCtx, String defaultSchema) {
        RelationName name = RelationName.of(createView.name(), defaultSchema);
        name.ensureValidForRelationCreation();
        if (BlobSchemaInfo.NAME.equals(name.schema())) {
            throw new UnsupportedOperationException("Creating a view in the \"blob\" schema is not supported");
        }
        QueriedRelation query = (QueriedRelation) relationAnalyzer.analyzeUnbound(
            createView.query(), txnCtx, ParamTypeHints.EMPTY);

        // sqlPrinter isn't feature complete yet; so restrict CREATE VIEW to only support queries where the
        // format->analyze round-trip works.
        String formattedQuery;
        try {
            formattedQuery = sqlPrinter.format(query);
            relationAnalyzer.analyzeUnbound((Query) SqlParser.createStatement(formattedQuery), txnCtx, ParamTypeHints.EMPTY);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Query cannot be used in a VIEW: " + createView.query());
        }

        // We do not bother with exists checks here because it wouldn't be "atomic" as it might be based
        // on an outdated cluster check, leading to a potential race condition.
        // The "masterOperation" which will update the clusterState will do a real-time verification

        if (query.fields().stream().map(Field::outputName).distinct().count() != query.fields().size()) {
            throw new IllegalArgumentException("Query in CREATE VIEW must not have duplicate column names");
        }
        return new CreateViewStmt(name, query, formattedQuery, createView.replaceExisting(), txnCtx.sessionContext().user());
    }
}
