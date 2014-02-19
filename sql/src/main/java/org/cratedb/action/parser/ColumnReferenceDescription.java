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

package org.cratedb.action.parser;

import org.cratedb.DataType;
import org.cratedb.index.ColumnDefinition;

public class ColumnReferenceDescription extends ColumnDescription {

    private DataType dataType;
    private String name;


    public ColumnReferenceDescription(ColumnDefinition columnDefinition) {
        this(columnDefinition.columnName, columnDefinition.dataType);
    }

    public ColumnReferenceDescription(String columnName, DataType dataType) {
        super(ColumnDescription.Types.CONSTANT_COLUMN);
        this.name = columnName;
        this.dataType = dataType;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataType returnType() {
        return dataType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ColumnReferenceDescription)) return false;

        ColumnReferenceDescription that = (ColumnReferenceDescription) o;

        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        return true;
    }

    @Override
    public int hashCode() {
        return name != null ? name.hashCode() : 0;
    }
}
