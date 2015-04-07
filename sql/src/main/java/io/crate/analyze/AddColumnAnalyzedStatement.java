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

import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

public class AddColumnAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    private final ReferenceInfos referenceInfos;
    private TableInfo tableInfo;
    private AnalyzedTableElements analyzedTableElements;
    private boolean newPrimaryKeys = false;

    protected AddColumnAnalyzedStatement(ReferenceInfos referenceInfos) {
        this.referenceInfos = referenceInfos;
    }

    public void table(TableIdent tableIdent) {
        tableInfo = referenceInfos.getWritableTable(tableIdent);
    }

    public TableInfo table() {
        return this.tableInfo;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> visitor, C context) {
        return visitor.visitAddColumnStatement(this, context);
    }

    public void analyzedTableElements(AnalyzedTableElements analyzedTableElements) {
        this.analyzedTableElements = analyzedTableElements;
    }

    public AnalyzedTableElements analyzedTableElements() {
        return this.analyzedTableElements;
    }

    public void newPrimaryKeys(boolean newPrimaryKeys) {
        this.newPrimaryKeys = newPrimaryKeys;
    }

    public boolean newPrimaryKeys() {
        return this.newPrimaryKeys;
    }
}
