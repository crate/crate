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

import com.google.common.collect.ImmutableList;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.table.TableInfo;
import io.crate.types.DataType;

import java.util.List;

public class InsertFromSubQueryAnalyzedStatement extends AbstractInsertAnalyzedStatement {

    private final AnalyzedRelation subQueryRelation;

    public InsertFromSubQueryAnalyzedStatement(AnalyzedRelation subQueryRelation, TableInfo targetTableInfo) {
        tableInfo(targetTableInfo);
        this.subQueryRelation = subQueryRelation;
    }

    public AnalyzedRelation subQueryRelation() {
        return this.subQueryRelation;
    }

    @Override
    public boolean hasNoResult() {
        return false;
    }

    @Override
    public void normalize() {
    }

    @Override
    public List<String> outputNames() {
        return ImmutableList.of();
    }

    @Override
    public List<DataType> outputTypes() {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitInsertFromSubQueryStatement(this, context);
    }
}
