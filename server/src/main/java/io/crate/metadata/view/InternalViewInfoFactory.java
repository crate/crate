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

import io.crate.analyze.ParamTypeHints;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.ResourceUnknownException;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.CoordinatorTxnCtx;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Query;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class InternalViewInfoFactory implements ViewInfoFactory {

    private Provider<RelationAnalyzer> analyzerProvider;

    @Inject
    public InternalViewInfoFactory(Provider<RelationAnalyzer> analyzerProvider) {
        this.analyzerProvider = analyzerProvider;
    }

    @Override
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
        try {
            AnalyzedRelation relation = analyzerProvider.get().analyze(
                (Query) SqlParser.createStatement(view.stmt()),
                CoordinatorTxnCtx.systemTransactionContext(),
                ParamTypeHints.EMPTY);
            final List<Reference> collectedColumns = new ArrayList<>(relation.outputs().size());
            int position = 1;
            for (var field : relation.outputs()) {
                collectedColumns.add(
                    new Reference(new ReferenceIdent(ident, Symbols.pathFromSymbol(field).sqlFqn()),
                                  RowGranularity.DOC,
                                  field.valueType(),
                                  position++,
                                  null));
            }
            columns = collectedColumns;
        } catch (ResourceUnknownException e) {
            // Return ViewInfo with no columns in case the statement could not be analyzed,
            // because the underlying table of the view could not be found.
            columns = Collections.emptyList();
        }
        return new ViewInfo(ident, view.stmt(), columns, view.owner());
    }
}
