/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import com.google.common.collect.Iterators;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.relations.RelationAnalyzer;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.symbol.Reference;
import io.crate.planner.symbol.Symbol;
import io.crate.planner.symbol.Symbols;
import io.crate.sql.tree.InsertFromSubquery;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Singleton;

import java.util.Locale;

@Singleton
public class InsertFromSubQueryAnalyzer extends AbstractInsertAnalyzer {


    private final RelationAnalyzer relationAnalyzer;

    @Inject
    protected InsertFromSubQueryAnalyzer(AnalysisMetaData analysisMetaData, RelationAnalyzer relationAnalyzer) {
        super(analysisMetaData);
        this.relationAnalyzer = relationAnalyzer;
    }

    @Override
    public AbstractInsertAnalyzedStatement visitInsertFromSubquery(InsertFromSubquery node, Analysis context) {
        if (!node.onDuplicateKeyAssignments().isEmpty()) {
            throw new UnsupportedOperationException("ON DUPLICATE KEY UPDATE clause is not supported");
        }
        TableInfo tableInfo = analysisMetaData.referenceInfos().getTableInfoUnsafe(TableIdent.of(node.table()));


        QueriedRelation source = (QueriedRelation) relationAnalyzer.analyze(node.subQuery(), context);
        InsertFromSubQueryAnalyzedStatement insertStatement =
                new InsertFromSubQueryAnalyzedStatement(source, tableInfo);

        // We forbid using limit/offset or order by until we've implemented ES paging support (aka 'scroll')
        // TODO: move this to the consumer
        if (source.querySpec().isLimited() || source.querySpec().orderBy() != null) {
            throw new UnsupportedFeatureException("Using limit, offset or order by is not" +
                    "supported on insert using a sub-query");
        }

        int numInsertColumns = node.columns().size() == 0 ? tableInfo.columns().size() : node.columns().size();
        int maxInsertValues = Math.max(numInsertColumns, source.fields().size());
        handleInsertColumns(node, maxInsertValues, insertStatement);

        validateMatchingColumns(insertStatement, source.querySpec());

        return insertStatement;
    }

    /**
     * validate that result columns from subquery match explicit insert columns
     * or complete table schema
     */
    private void validateMatchingColumns(InsertFromSubQueryAnalyzedStatement context, QuerySpec querySpec) {
        if (context.columns().size() != querySpec.outputs().size()) {
            throw new IllegalArgumentException("Number of columns in insert statement and subquery differ");
        }

        int failedCastPosition = querySpec.castOutputs(Iterators.transform(context.columns().iterator(), Symbols.TYPES_FUNCTION));
        if (failedCastPosition >= 0) {
            Symbol failedSource = querySpec.outputs().get(failedCastPosition);
            Reference failedTarget = context.columns().get(failedCastPosition);
            throw new IllegalArgumentException(String.format(Locale.ENGLISH,
                    "Type of subquery column %s (%s) does not match is not convertable to the type of table column %s (%s)",
                    failedSource,
                    failedSource.valueType(),
                    failedTarget.info().ident().columnIdent().fqn(),
                    failedTarget.valueType()
            ));
        }
    }

}
