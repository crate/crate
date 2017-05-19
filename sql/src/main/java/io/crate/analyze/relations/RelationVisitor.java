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

package io.crate.analyze.relations;

import io.crate.analyze.*;

import javax.annotation.Nullable;
import java.util.Locale;

public abstract class RelationVisitor<C, R> {

    public R process(QueriedRelation relation, @Nullable C context) {
        return relation.accept(this, context);
    }

    protected R visitRelation(QueriedRelation relation, C context) {
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "relation \"%s\" is not supported", relation));
    }

    public R visitQueriedTable(QueriedTable table, C context) {
        return visitRelation(table, context);
    }

    public R visitQueriedDocTable(QueriedDocTable table, C context) {
        return visitRelation(table, context);
    }

    public R visitMultiSourceSelect(MultiSourceSelect multiSourceSelect, C context) {
        return visitRelation(multiSourceSelect, context);
    }

    public R visitTableRelation(TableRelation tableRelation, C context) {
        return visitRelation(tableRelation, context);
    }

    public R visitDocTableRelation(DocTableRelation relation, C context) {
        return visitRelation(relation, context);
    }

    public R visitTwoTableJoin(TwoTableJoin twoTableJoin, C context) {
        return visitRelation(twoTableJoin, context);
    }

    public R visitExplain(ExplainAnalyzedStatement explainAnalyzedStatement, C context) {
        return visitRelation(explainAnalyzedStatement, context);
    }

    public R visitTableFunctionRelation(TableFunctionRelation tableFunctionRelation, C context) {
        return visitRelation(tableFunctionRelation, context);
    }

    public R visitQueriedSelectRelation(QueriedSelectRelation relation, C context) {
        return visitRelation(relation, context);
    }
}
