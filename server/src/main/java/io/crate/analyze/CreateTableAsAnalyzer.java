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

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.analyze.relations.StatementAnalysisContext;
import io.crate.common.collections.Lists;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateTableAs;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.Insert;
import io.crate.sql.tree.TableElement;

public final class CreateTableAsAnalyzer {

    private final CreateTableStatementAnalyzer createTableStatementAnalyzer;
    private final InsertAnalyzer insertAnalyzer;
    private final RelationAnalyzer relationAnalyzer;

    public CreateTableAsAnalyzer(CreateTableStatementAnalyzer createTableStatementAnalyzer,
                                 InsertAnalyzer insertAnalyzer,
                                 RelationAnalyzer relationAnalyzer) {

        this.createTableStatementAnalyzer = createTableStatementAnalyzer;
        this.insertAnalyzer = insertAnalyzer;
        this.relationAnalyzer = relationAnalyzer;
    }

    public AnalyzedCreateTableAs analyze(CreateTableAs<Expression> createTableAs,
                                         ParamTypeHints paramTypeHints,
                                         CoordinatorTxnCtx txnCtx) {

        RelationName relationName = RelationName.of(
            createTableAs.name().getName(),
            txnCtx.sessionSettings().searchPath().currentSchema());

        relationName.ensureValidForRelationCreation();

        AnalyzedRelation analyzedSourceQuery = relationAnalyzer.analyze(
            createTableAs.query(),
            new StatementAnalysisContext(paramTypeHints, Operation.READ, txnCtx));

        List<TableElement<Expression>> tableElements =
            Lists.map(analyzedSourceQuery.outputs(), Symbol::toColumnDefinition);

        CreateTable<Expression> createTable = new CreateTable<Expression>(
            createTableAs.name(),
            tableElements,
            Optional.empty(),
            Optional.empty(),
            GenericProperties.empty(),
            createTableAs.ifNotExists());

        // This is only a preliminary analysis to to have the source available for privilege checks.
        // It will be analyzed again with the target columns from the target table once
        // the table has been created.
        AnalyzedRelation sourceRelation = relationAnalyzer.analyze(
            createTableAs.query(),
            new StatementAnalysisContext(paramTypeHints, Operation.READ, txnCtx)
        );

        //postponing the analysis of the insert statement, since the table has not been created yet.
        Supplier<AnalyzedInsertStatement> postponedInsertAnalysis = () -> {
            Insert<Expression> insert = new Insert<Expression>(
                createTableAs.name(),
                createTableAs.query(),
                Collections.emptyList(),
                Collections.emptyList(),
                Insert.DuplicateKeyContext.none());

            return insertAnalyzer.analyze(insert, paramTypeHints, txnCtx);
        };

        return new AnalyzedCreateTableAs(
            createTableStatementAnalyzer.analyze(createTable, paramTypeHints, txnCtx),
            sourceRelation,
            postponedInsertAnalysis
        );
    }
}

