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
import java.util.List;
import java.util.Locale;

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.inject.Singleton;

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Query;

@Singleton
public class ViewInfoFactory {

    private final Provider<RelationAnalyzer> analyzerProvider;

    public ViewInfoFactory(Provider<RelationAnalyzer> analyzerProvider) {
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
        boolean analyzeError = false;
        try {
            CoordinatorTxnCtx transactionContext = CoordinatorTxnCtx.systemTransactionContext();
            transactionContext.sessionSettings().setSearchPath(view.searchPath());
            AnalyzedRelation relation = analyzerProvider.get().analyze(
                (Query) SqlParser.createStatement(view.stmt()),
                transactionContext,
                ParamTypeHints.EMPTY
            );
            final List<Reference> collectedColumns = new ArrayList<>(relation.outputs().size());
            int position = 1;
            for (var field : relation.outputs()) {
                collectedColumns.add(
                    new SimpleReference(new ReferenceIdent(ident, Symbols.pathFromSymbol(field).sqlFqn()),
                                  RowGranularity.DOC,
                                  field.valueType(),
                                  position++,
                                  null));
            }
            columns = collectedColumns;
        } catch (Exception e) {
            // Statement could not be analyzed, because the referenced table either not found
            // or has been updated and view definition became incompatible with the new schema (https://github.com/crate/crate/issues/14377).
            columns = Collections.emptyList();
            analyzeError = true;
        }
        String viewDefinition = analyzeError ? String.format(Locale.ENGLISH, "/* Corrupted view, needs fix */\n%s", view.stmt()) : view.stmt();
        return new ViewInfo(ident, viewDefinition, columns, view.owner(), view.searchPath());
    }
}
