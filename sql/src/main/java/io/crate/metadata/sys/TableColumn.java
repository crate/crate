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

package io.crate.metadata.sys;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.metadata.table.TableInfo;

public class TableColumn {

    private final TableInfo tableInfo;
    private final String name;

    public TableColumn(TableInfo tableInfo, String name) {
        this.tableInfo = tableInfo;
        this.name = name;
    }

    public ReferenceInfo getReferenceInfo(TableIdent tableIdent, ColumnIdent columnIdent) {
        if (!name.equals(columnIdent.name())) {
            return null;
        }
        ColumnIdent myColumnIdent = getImplementationIdent(columnIdent);
        ReferenceInfo info = tableInfo.getReferenceInfo(myColumnIdent);
        if (info != null) {
            return info.getRelocated(new ReferenceIdent(tableIdent, columnIdent));
        }
        return null;
    }

    private ColumnIdent getImplementationIdent(ColumnIdent columnIdent) {
        ColumnIdent myColumnIdent;
        if (columnIdent.isColumn()) {
            myColumnIdent = new ColumnIdent(name);
        } else {
            myColumnIdent = new ColumnIdent(
                    columnIdent.path().get(0), columnIdent.path().subList(1, columnIdent.path().size()));
        }
        return myColumnIdent;
    }
}