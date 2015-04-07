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

import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.metadata.ReferenceInfos;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

public abstract class AbstractDropTableAnalyzedStatement extends AbstractDDLAnalyzedStatement {

    protected final ReferenceInfos referenceInfos;
    protected final boolean dropIfExists;

    protected TableInfo tableInfo;
    protected boolean noop;

    public AbstractDropTableAnalyzedStatement(ReferenceInfos referenceInfos, boolean dropIfExists) {
        this.referenceInfos = referenceInfos;
        this.dropIfExists = dropIfExists;
    }

    public String index() {
        return tableIdent().esName();
    }

    public TableInfo table() {
        return tableInfo;
    }

    public TableIdent tableIdent(){
        return tableInfo.ident();
    }

    public void table(TableIdent tableIdent) {
        try {
            tableInfo = referenceInfos.getWritableTable(tableIdent);
        } catch (SchemaUnknownException | TableUnknownException e) {
            if (dropIfExists) {
                noop = true;
            } else {
                throw e;
            }
        }
    }

    public boolean noop(){
        return noop;
    }
    public boolean dropIfExists() {
        return dropIfExists;
    }

    @Override
    public abstract <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context);
}
