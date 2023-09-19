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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import io.crate.analyze.AnalyzedAlterTableDropColumn.DropColumn;
import io.crate.analyze.expressions.ExpressionAnalysisContext;
import io.crate.analyze.expressions.ExpressionAnalyzer;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.analyze.relations.NameFieldProvider;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.Schemas;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.AlterTableDropColumn;
import io.crate.sql.tree.Expression;
import org.elasticsearch.Version;

public class AlterTableDropColumnAnalyzer {

    private final Schemas schemas;
    private final NodeContext nodeCtx;

    AlterTableDropColumnAnalyzer(Schemas schemas, NodeContext nodeCtx) {
        this.schemas = schemas;
        this.nodeCtx = nodeCtx;
    }

    public AnalyzedAlterTableDropColumn analyze(AlterTableDropColumn<Expression> alterTable,
                                                ParamTypeHints paramTypeHints,
                                                CoordinatorTxnCtx txnCtx) {
        if (!alterTable.table().partitionProperties().isEmpty()) {
            throw new UnsupportedOperationException("Dropping a column from a single partition is not supported");
        }

        DocTableInfo tableInfo = (DocTableInfo) schemas.resolveTableInfo(
            alterTable.table().getName(),
            Operation.ALTER,
            txnCtx.sessionSettings().sessionUser(),
            txnCtx.sessionSettings().searchPath());

        var expressionAnalyzer = new ExpressionAnalyzer(
            txnCtx,
            nodeCtx,
            paramTypeHints,
            new NameFieldProvider(new DocTableRelation(tableInfo)),
            null
        );
        var expressionContext = new ExpressionAnalysisContext(txnCtx.sessionSettings());
        List<DropColumn> dropColumns = new ArrayList<>(alterTable.tableElements().size());

        for (var dropColumnDefinition : alterTable.tableElements()) {
            Expression name = dropColumnDefinition.name();
            try {
                var colRefToDrop = (Reference) expressionAnalyzer.convert(name, expressionContext);
                dropColumns.add(new DropColumn(colRefToDrop, dropColumnDefinition.ifExists()));
            } catch (ColumnUnknownException e) {
                if (dropColumnDefinition.ifExists() == false) {
                    throw e;
                }
            }
        }
        dropColumns = validateDynamic(tableInfo, dropColumns);
        validateStatic(tableInfo, dropColumns);

        return new AnalyzedAlterTableDropColumn(tableInfo, dropColumns);
    }


    /** Validate restrictions based on properties that cannot change */
    private static void validateStatic(DocTableInfo tableInfo, List<DropColumn> dropColumns) {
        if (tableInfo.versionCreated().before(Version.V_5_5_0)) {
            throw new UnsupportedOperationException(
                "Dropping columns of a table created before version 5.5 is not supported"
            );
        }
        Set<ColumnIdent> uniqueSet = new HashSet<>(dropColumns.size());
        for (int i = 0 ; i < dropColumns.size(); i++) {
            var refToDrop = dropColumns.get(i).ref();
            var colToDrop = refToDrop.column();

            if (uniqueSet.contains(colToDrop)) {
                throw new IllegalArgumentException("Column \"" + colToDrop.sqlFqn() + "\" specified more than once");
            }
            uniqueSet.add(colToDrop);

            for (var indexRef : tableInfo.indexColumns()) {
                if (indexRef.columns().contains(refToDrop)) {
                    throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                            "is part of INDEX: " + indexRef +
                                                            " is not allowed");
                }
            }
            if (tableInfo.primaryKey().contains(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is part of the PRIMARY KEY is not allowed");
            }
            if (tableInfo.clusteredBy().equals(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is used in 'CLUSTERED BY' is not allowed");
            }
            if (tableInfo.isPartitioned() && tableInfo.partitionedBy().contains(colToDrop)) {
                throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                        "is part of the 'PARTITIONED BY' columns is not allowed");
            }
        }
    }

    /** Validate restrictions based on properties that change and need to be rechecked during execution*/
    public static List<DropColumn> validateDynamic(DocTableInfo tableInfo, List<DropColumn> dropColumns) {
        var generatedColRefs = new HashSet<>();
        for (var genRef : tableInfo.generatedColumns()) {
            generatedColRefs.addAll(genRef.referencedReferences());
        }
        var leftOverCols = tableInfo.columns().stream().map(Reference::column).collect(Collectors.toSet());
        ArrayList<DropColumn> validatedDropCols = new ArrayList<>(dropColumns.size());

        for (int i = 0 ; i < dropColumns.size(); i++) {
            var refToDrop = dropColumns.get(i).ref();
            var colToDrop = refToDrop.column();

            for (var indexRef : tableInfo.indexColumns()) {
                if (indexRef.columns().contains(refToDrop)) {
                    throw new UnsupportedOperationException("Dropping column: " + colToDrop.sqlFqn() + " which " +
                                                            "is part of INDEX: " + indexRef + " is not allowed");
                }
            }

            if (generatedColRefs.contains(refToDrop)) {
                throw new UnsupportedOperationException(
                    "Dropping column: " + colToDrop.sqlFqn() + " which is used to produce values for " +
                    "generated column is not allowed");
            }
            leftOverCols.remove(colToDrop);
            validatedDropCols.add(new DropColumn(refToDrop, dropColumns.get(i).ifExists()));
        }

        if (leftOverCols.isEmpty()) {
            throw new UnsupportedOperationException("Dropping all columns of a table is not allowed");
        }
        return validatedDropCols;
    }
}
