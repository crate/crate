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
import java.util.List;
import java.util.Set;

import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.RelationInfo;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.settings.CoordinatorSessionSettings;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CheckColumnConstraint;
import io.crate.sql.tree.CheckConstraint;
import io.crate.sql.tree.ColumnConstraint;
import io.crate.sql.tree.ColumnDefinition;
import io.crate.sql.tree.ColumnStorageDefinition;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.CreateTableLike;
import io.crate.sql.tree.DefaultConstraint;
import io.crate.sql.tree.Expression;
import io.crate.sql.tree.GenericProperties;
import io.crate.sql.tree.GeneratedExpressionConstraint;
import io.crate.sql.tree.IndexColumnConstraint;
import io.crate.sql.tree.IndexDefinition;
import io.crate.sql.tree.LikeOption;
import io.crate.sql.tree.NotNullColumnConstraint;
import io.crate.sql.tree.PrimaryKeyColumnConstraint;
import io.crate.sql.tree.PrimaryKeyConstraint;
import io.crate.sql.tree.TableElement;

public final class CreateTableLikeAnalyzer {

    private final Schemas schemas;
    private final CreateTableStatementAnalyzer createTableStatementAnalyzer;

    public CreateTableLikeAnalyzer(Schemas schemas,
                                   CreateTableStatementAnalyzer createTableStatementAnalyzer) {
        this.schemas = schemas;
        this.createTableStatementAnalyzer = createTableStatementAnalyzer;
    }

    public AnalyzedCreateTableLike analyze(CreateTableLike<Expression> createTableLike,
                                           ParamTypeHints paramTypeHints,
                                           CoordinatorTxnCtx txnCtx) {

        RelationName relationName = RelationName.of(
            createTableLike.name().getName(),
            txnCtx.sessionSettings().searchPath().currentSchema());
        relationName.ensureValidForRelationCreation();

        CoordinatorSessionSettings sessionSettings = txnCtx.sessionSettings();
        RelationInfo relationInfo = schemas.findRelation(
            createTableLike.likeTableName(),
            Operation.SHOW_CREATE,
            sessionSettings.sessionUser(),
            sessionSettings.searchPath()
        );

        if (!(relationInfo instanceof DocTableInfo sourceTableInfo)) {
            throw new UnsupportedOperationException(
                "Cannot use CREATE TABLE LIKE on relation " + relationInfo.ident() +
                " of type " + relationInfo.relationType());
        }

        CreateTable<Expression> sourceCreateTable =
            (CreateTable<Expression>) new TableInfoToAST(sourceTableInfo).toStatement();

        Set<LikeOption> includedOptions = createTableLike.includedOptions();
        List<TableElement<Expression>> filteredElements =
            filterTableElements(sourceCreateTable.tableElements(), includedOptions);

        CreateTable<Expression> newCreateTable = new CreateTable<>(
            createTableLike.name(),
            filteredElements,
            sourceCreateTable.partitionedBy(),
            sourceCreateTable.clusteredBy(),
            includedOptions.contains(LikeOption.STORAGE)
                ? sourceCreateTable.properties()
                : GenericProperties.empty(),
            createTableLike.ifNotExists()
        );

        AnalyzedCreateTable analyzedCreateTable =
            createTableStatementAnalyzer.analyze(newCreateTable, paramTypeHints, txnCtx);

        return new AnalyzedCreateTableLike(analyzedCreateTable, sourceTableInfo);
    }

    private static List<TableElement<Expression>> filterTableElements(
            List<TableElement<Expression>> sourceElements,
            Set<LikeOption> includedOptions) {

        List<TableElement<Expression>> filtered = new ArrayList<>();
        for (TableElement<Expression> element : sourceElements) {
            if (element instanceof ColumnDefinition<Expression> colDef) {
                filtered.add(filterColumnDefinition(colDef, includedOptions));
            } else if (element instanceof PrimaryKeyConstraint) {
                if (includedOptions.contains(LikeOption.CONSTRAINTS)) {
                    filtered.add(element);
                }
            } else if (element instanceof CheckConstraint) {
                if (includedOptions.contains(LikeOption.CONSTRAINTS)) {
                    filtered.add(element);
                }
            } else if (element instanceof IndexDefinition) {
                if (includedOptions.contains(LikeOption.INDEXES)) {
                    filtered.add(element);
                }
            } else {
                filtered.add(element);
            }
        }
        return filtered;
    }

    private static ColumnDefinition<Expression> filterColumnDefinition(
            ColumnDefinition<Expression> colDef,
            Set<LikeOption> includedOptions) {

        List<ColumnConstraint<Expression>> filteredConstraints = new ArrayList<>();
        for (ColumnConstraint<Expression> constraint : colDef.constraints()) {
            if (constraint instanceof NotNullColumnConstraint) {
                // NOT NULL is always copied (PostgreSQL behavior)
                filteredConstraints.add(constraint);
            } else if (constraint instanceof PrimaryKeyColumnConstraint) {
                if (includedOptions.contains(LikeOption.CONSTRAINTS)) {
                    filteredConstraints.add(constraint);
                }
            } else if (constraint instanceof CheckColumnConstraint) {
                if (includedOptions.contains(LikeOption.CONSTRAINTS)) {
                    filteredConstraints.add(constraint);
                }
            } else if (constraint instanceof DefaultConstraint) {
                if (includedOptions.contains(LikeOption.DEFAULTS)) {
                    filteredConstraints.add(constraint);
                }
            } else if (constraint instanceof GeneratedExpressionConstraint) {
                if (includedOptions.contains(LikeOption.GENERATED)) {
                    filteredConstraints.add(constraint);
                }
            } else if (constraint instanceof IndexColumnConstraint) {
                if (includedOptions.contains(LikeOption.INDEXES)) {
                    filteredConstraints.add(constraint);
                }
            } else if (constraint instanceof ColumnStorageDefinition) {
                if (includedOptions.contains(LikeOption.STORAGE)) {
                    filteredConstraints.add(constraint);
                }
            } else {
                filteredConstraints.add(constraint);
            }
        }
        return new ColumnDefinition<>(colDef.ident(), colDef.type(), filteredConstraints);
    }
}
