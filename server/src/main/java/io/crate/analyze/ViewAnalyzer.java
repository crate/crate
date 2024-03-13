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

package io.crate.analyze;

import java.util.ArrayList;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.RelationUnknown;
import io.crate.exceptions.RelationsUnknown;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.SqlFormatter;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.CreateView;
import io.crate.sql.tree.DropView;
import io.crate.sql.tree.QualifiedName;
import io.crate.sql.tree.Query;

public final class ViewAnalyzer {

    private final RelationAnalyzer relationAnalyzer;
    private final Schemas schemas;

    ViewAnalyzer(RelationAnalyzer relationAnalyzer, Schemas schemas) {
        this.relationAnalyzer = relationAnalyzer;
        this.schemas = schemas;
    }

    public CreateViewStmt analyze(CreateView createView, CoordinatorTxnCtx txnCtx) {
        RelationName name = RelationName.of(createView.name(), txnCtx.sessionSettings().searchPath().currentSchema());
        name.ensureValidForRelationCreation();
        if (BlobSchemaInfo.NAME.equals(name.schema())) {
            throw new UnsupportedOperationException("Creating a view in the \"blob\" schema is not supported");
        }
        AnalyzedRelation query;
        String formattedQuery;
        try {
            formattedQuery = SqlFormatter.formatSql(createView.query());
        } catch (Exception e) {
            throw new UnsupportedOperationException("Invalid query used in CREATE VIEW. Query: " + createView.query());
        }
        try {
            // Analyze the formatted Query to make sure the formatting didn't mess it up in any way.
            query = relationAnalyzer.analyze(
                (Query) SqlParser.createStatement(formattedQuery),
                txnCtx,
                ParamTypeHints.EMPTY);
        } catch (Exception e) {
            throw new UnsupportedOperationException("Invalid query used in CREATE VIEW. " + e.getMessage() + ". Query: " + formattedQuery);
        }

        // We do not bother with exists checks here because it wouldn't be "atomic" as it might be based
        // on an outdated cluster check, leading to a potential race condition.
        // The "masterOperation" which will update the clusterState will do a real-time verification

        if (query.outputs().stream().map(f -> Symbols.pathFromSymbol(f).sqlFqn()).distinct().count() != query.outputs().size()) {
            throw new IllegalArgumentException("Query in CREATE VIEW must not have duplicate column names");
        }
        return new CreateViewStmt(
            name,
            query,
            createView.query(),
            createView.replaceExisting(),
            txnCtx.sessionSettings().sessionUser()
        );
    }

    public AnalyzedDropView analyze(DropView dropView, CoordinatorTxnCtx txnCtx) {
        // No exists check to avoid stale clusterState race conditions
        ArrayList<RelationName> views = new ArrayList<>(dropView.names().size());
        ArrayList<RelationName> missing = new ArrayList<>();
        for (QualifiedName qualifiedName : dropView.names()) {
            try {
                views.add(schemas.findView(qualifiedName, txnCtx.sessionSettings().searchPath()).name());
            } catch (RelationUnknown e) {
                if (!dropView.ifExists()) {
                    missing.add(RelationName.of(qualifiedName, txnCtx.sessionSettings().searchPath().currentSchema()));
                }
            }
        }

        if (!missing.isEmpty()) {
            throw new RelationsUnknown(missing);
        }

        return new AnalyzedDropView(views, dropView.ifExists());
    }
}
