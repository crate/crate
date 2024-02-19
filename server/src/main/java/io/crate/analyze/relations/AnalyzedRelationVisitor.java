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

package io.crate.analyze.relations;

import java.util.Locale;

import io.crate.analyze.AnalyzedShowCreateTable;
import io.crate.analyze.ExplainAnalyzedStatement;
import io.crate.analyze.QueriedSelectRelation;
import io.crate.fdw.ForeignTableRelation;

public abstract class AnalyzedRelationVisitor<C, R> {

    protected R visitAnalyzedRelation(AnalyzedRelation relation, C context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "relation \"%s\" is not supported", relation));
    }

    public R visitUnionSelect(UnionSelect unionSelect, C context) {
        return visitAnalyzedRelation(unionSelect, context);
    }

    public R visitTableRelation(TableRelation tableRelation, C context) {
        return visitAnalyzedRelation(tableRelation, context);
    }

    public R visitDocTableRelation(DocTableRelation relation, C context) {
        return visitAnalyzedRelation(relation, context);
    }

    public R visitExplain(ExplainAnalyzedStatement explainAnalyzedStatement, C context) {
        return visitAnalyzedRelation(explainAnalyzedStatement, context);
    }

    public R visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, C context) {
        return visitAnalyzedRelation(tableFunctionRelation, context);
    }

    public R visitQueriedSelectRelation(QueriedSelectRelation relation, C context) {
        return visitAnalyzedRelation(relation, context);
    }

    public R visitView(AnalyzedView analyzedView, C context) {
        return analyzedView.relation().accept(this, context);
    }

    public R visitAliasedAnalyzedRelation(AliasedAnalyzedRelation relation, C context) {
        return relation.relation().accept(this, context);
    }

    public R visitShowCreateTable(AnalyzedShowCreateTable analyzedShowCreateTable, C context) {
        return visitAnalyzedRelation(analyzedShowCreateTable, context);
    }

    public R visitForeignTable(ForeignTableRelation foreignTableRelation, C context) {
        return visitAnalyzedRelation(foreignTableRelation, context);
    }
}
