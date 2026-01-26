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

package io.crate.metadata.view;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;

import org.elasticsearch.cluster.ClusterState;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedView;
import io.crate.analyze.relations.FieldResolver;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.expression.symbol.ScopedColumn;
import io.crate.expression.symbol.ScopedSymbol;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Query;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class ViewInfoFactory {

    private final RelationAnalyzer analyzerProvider;

    public ViewInfoFactory(RelationAnalyzer analyzerProvider) {
        this.analyzerProvider = analyzerProvider;
    }

    public ViewInfo create(RelationName ident, ClusterState state) {
        ViewsMetadata meta = state.metadata().custom(ViewsMetadata.TYPE);
        if (meta == null) {
            return null;
        }
        ViewMetadata view = meta.getView(ident);
        if (view == null) {
            return null;
        }
        List<Reference> columns;
        LinkedHashSet<ScopedColumn> usedSourceColumns = new LinkedHashSet<>();
        boolean analyzeError = false;
        boolean errorOnUnknownObjectKey = view.errorOnUnknownObjectKey();
        try {
            CoordinatorTxnCtx transactionContext = CoordinatorTxnCtx.systemTransactionContext();
            transactionContext.sessionSettings().setSearchPath(view.searchPath());
            transactionContext.sessionSettings().setErrorOnUnknownObjectKey(errorOnUnknownObjectKey);

            AnalyzedRelation relation = analyzerProvider.analyze(
                (Query) SqlParser.createStatement(view.stmt()),
                transactionContext,
                ParamTypeHints.EMPTY
            );
            HashMap<RelationName, AnalyzedRelation> from = new HashMap<>();
            if (relation instanceof QueriedSelectRelation selectRelation) {
                for (AnalyzedRelation rel : selectRelation.from()) {
                    from.put(rel.relationName(), rel);
                }
            }
            List<Symbol> outputs = relation.outputs();
            columns = new ArrayList<>(outputs.size());
            int position = 1;
            for (var field : outputs) {
                ColumnIdent columnIdent = field.toColumn();
                columns.add(
                    new SimpleReference(
                        ident,
                        ColumnIdent.of(columnIdent.sqlFqn()),
                        RowGranularity.DOC,
                        field.valueType(),
                        position++,
                        null
                    )
                );
                // Peek into in-line aliased relations like in:
                //      create view v1 as (select * from t1, (select x as y from t1) t2)
                // To only consider "t1.x" as used column
                field.any(x -> {
                    if (x instanceof Reference ref) {
                        usedSourceColumns.add(ref);
                    } else if (x instanceof ScopedSymbol scopedSymbol) {
                        AnalyzedRelation sourceRel = from.get(scopedSymbol.relation());
                        if (sourceRel instanceof AnalyzedView || sourceRel == null) {
                            usedSourceColumns.add(scopedSymbol);
                        } else if (sourceRel instanceof FieldResolver fieldResolver) {
                            Symbol innerField = fieldResolver.resolveField(scopedSymbol);
                            if (innerField != null) {
                                innerField.visit(Reference.class, ix -> usedSourceColumns.add(ix));
                            }
                        }
                    }
                    return false;
                });
            }
            // Now add all sub-columns.
            // We do it after handling top level columns to ensure that ordinals in the information_schema.columns are stable
            // and sub-columns, added or dropped after view definition don't change ordinals of other columns in the view.
            int size = columns.size();
            for (int i = 0; i < size; i++) {
                Reference ref = columns.get(i);
                position = addSubColumns(columns, ident, ref.column(), ref.valueType(), position);
            }
        } catch (Exception e) {
            // Statement could not be analyzed, because the referenced table either not found
            // or has been updated and view definition became incompatible with the new schema (https://github.com/crate/crate/issues/14377).
            columns = Collections.emptyList();
            analyzeError = true;
        }
        String viewDefinition = analyzeError ? String.format(Locale.ENGLISH, "/* Corrupted view, needs fix */\n%s", view.stmt()) : view.stmt();
        return new ViewInfo(
            ident,
            viewDefinition,
            columns,
            List.copyOf(usedSourceColumns),
            view.owner(),
            view.searchPath(),
            errorOnUnknownObjectKey
        );
    }

    private static int addSubColumns(List<Reference> subColumns,
                                     RelationName ident,
                                     ColumnIdent parent,
                                     DataType<?> parentType,
                                     int position) {
        int updatedPosition = position;
        if (parentType instanceof ObjectType objectType) {
            for (var entry : objectType.innerTypes().entrySet()) {
                String childName = entry.getKey();
                ColumnIdent childColumn = parent.getChild(childName);
                DataType<?> childType = entry.getValue();
                subColumns.add(
                    new SimpleReference(
                        ident,
                        ColumnIdent.of(childColumn.sqlFqn()),
                        RowGranularity.DOC,
                        childType,
                        position++,
                        null
                    )
                );
                updatedPosition = addSubColumns(subColumns, ident, childColumn, childType, position);
            }
        }
        return updatedPosition;
    }
}
