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

package io.crate.metadata;

import com.google.common.base.Objects;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;

public class ReferenceIdent implements Streamable {

    private TableIdent tableIdent;
    private ColumnIdent columnIdent;

    public ReferenceIdent() {

    }

    public ReferenceIdent(TableIdent tableIdent, ColumnIdent columnIdent) {
        this.tableIdent = tableIdent;
        this.columnIdent = columnIdent;
    }

    public ReferenceIdent(TableIdent tableIdent, String column) {
        this(tableIdent, new ColumnIdent(column));
    }

    public ReferenceIdent(TableIdent tableIdent, String column, @Nullable List<String> path) {
        this(tableIdent, new ColumnIdent(column, path));
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public boolean isColumn() {
        return columnIdent.isColumn();
    }

    public ColumnIdent columnIdent(){
        return columnIdent;
    }

    public ReferenceIdent columnReferenceIdent() {
        if (isColumn()) {
            return this;
        }
        return new ReferenceIdent(tableIdent, columnIdent.name());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        ReferenceIdent o = (ReferenceIdent) obj;
        return Objects.equal(columnIdent, o.columnIdent) &&
                Objects.equal(tableIdent, o.tableIdent);
    }

    @Override
    public int hashCode() {
        int result = tableIdent.hashCode();
        result = 31 * result + columnIdent.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format("<RefIdent: %s->%s>", tableIdent, columnIdent);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        columnIdent = new ColumnIdent();
        columnIdent.readFrom(in);
        tableIdent = TableIdent.fromStream(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        columnIdent.writeTo(out);
        tableIdent.writeTo(out);
    }
}
