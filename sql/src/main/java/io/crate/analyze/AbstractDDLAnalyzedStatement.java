/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import io.crate.Constants;
import io.crate.exceptions.InvalidTableNameException;
import io.crate.metadata.TableIdent;

public abstract class AbstractDDLAnalyzedStatement extends AnalyzedStatement {

    protected final TableParameter tableParameter = new TableParameter();
    protected TableIdent tableIdent;

    protected AbstractDDLAnalyzedStatement(ParameterContext parameterContext) {
        super(parameterContext);
    }

    public void table(TableIdent tableIdent) {
        if (!isValidTableName(tableIdent.name())) {
            throw new InvalidTableNameException(tableIdent.name());
        }
        this.tableIdent = tableIdent;
    }

    @Override
    public boolean hasNoResult() {
        return false;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDDLAnalyzedStatement(this, context);
    }

    @Override
    public boolean expectsAffectedRows() {
        return true;
    }

    public boolean isValidTableName(String name) {
        for (String illegalCharacter: Constants.INVALID_TABLE_NAME_CHARACTERS) {
            if (name.contains(illegalCharacter)) {
                return false;
            }
        }
        return true;
    }

    public TableParameter tableParameter() {
        return tableParameter;
    }
}
