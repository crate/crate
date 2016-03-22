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

import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.metadata.OutputName;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.types.DataTypes;

import java.util.Collections;
import java.util.List;
import java.util.Locale;

public class ShowCreateTableAnalyzedStatement extends AbstractShowAnalyzedStatement {

    private final DocTableInfo tableInfo;
    private final List<Field> fields;

    public ShowCreateTableAnalyzedStatement(DocTableInfo tableInfo) {
        String columnName = String.format(Locale.ENGLISH, "SHOW CREATE TABLE %s", tableInfo.ident().fqn());
        this.fields = Collections.singletonList(new Field(this, new OutputName(columnName), DataTypes.STRING));
        this.tableInfo = tableInfo;
    }

    public DocTableInfo tableInfo() {
        return tableInfo;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitShowCreateTableAnalyzedStatement(this, context);
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.process(this, context);
    }

    @Override
    public List<Field> fields() {
        return fields;
    }

    @Override
    public boolean isWriteOperation() {
        return false;
    }
}
