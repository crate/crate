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

package io.crate.metadata.view;

import io.crate.analyze.ParameterContext;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.TransactionContext;
import io.crate.sql.parser.SqlParser;
import io.crate.sql.tree.Statement;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;

import java.util.ArrayList;

public class InternalViewInfoFactory implements ViewInfoFactory {

    private Provider<RelationAnalyzer> analyzerProvider;

    @Inject
    public InternalViewInfoFactory(Provider<RelationAnalyzer> analyzerProvider) {
        this.analyzerProvider = analyzerProvider;
    }

    @Override
    public ViewInfo create(TableIdent ident, ClusterState state) {
        ViewsMetaData meta = state.metaData().custom(ViewsMetaData.TYPE);
        if (meta == null) {
            return null;
        }
        String statement = meta.getStatement(ident);
        if (statement == null) {
            return null;
        }
        Statement parsedStmt = SqlParser.createStatement(statement);
        AnalyzedRelation relation = analyzerProvider.get().analyze(parsedStmt, new TransactionContext(), ParameterContext.EMPTY);
        ArrayList<Reference> columns = new ArrayList<>(relation.fields().size());
        relation.fields().forEach(field -> columns.add(new Reference(new ReferenceIdent(ident, field.outputName()), RowGranularity.DOC, field.valueType())));
        return new ViewInfo(ident, columns);
    }
}
