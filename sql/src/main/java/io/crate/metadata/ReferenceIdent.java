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
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ReferenceIdent implements Comparable<ReferenceIdent>, Streamable {

    private TableIdent tableIdent;
    private String column;
    private List<String> path;

    public ReferenceIdent() {

    }

    public ReferenceIdent(TableIdent tableIdent, String column) {
        this.tableIdent = tableIdent;
        this.column = column;
        this.path = new ArrayList<>(0);
    }

    public ReferenceIdent(TableIdent tableIdent, String column, List<String> path) {
        this(tableIdent, column);
        this.path = path;
    }

    public TableIdent tableIdent() {
        return tableIdent;
    }

    public String column() {
        return column;
    }

    public List<String> path() {
        return path;
    }

    public boolean isColumn() {
        return path.size() == 0;
    }

    public ReferenceIdent columnIdent(){
        if (isColumn()){
            return this;
        }
        return new ReferenceIdent(tableIdent, column);
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
        return Objects.equal(column, o.column) &&
                Objects.equal(tableIdent, o.tableIdent) &&
                Objects.equal(path, o.path);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(tableIdent, column, path);
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(this)
                .add("table", tableIdent)
                .add("column", column)
                .add("path", path)
                .toString();
    }

    @Override
    public int compareTo(ReferenceIdent o) {
        return ComparisonChain.start()
                .compare(tableIdent, o.tableIdent)
                .compare(column, o.column)
                .compare(path, o.path, Ordering.<String>natural().lexicographical())
                .result();
    }


    @Override
    public void readFrom(StreamInput in) throws IOException {
        column = in.readString();
        int numParts = in.readVInt();
        path = new ArrayList<>(numParts);
        for (int i = 0; i < numParts; i++) {
            path.add(in.readString());
        }

        tableIdent = new TableIdent();
        tableIdent.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(column);
        out.writeVInt(path.size());
        for (String s : path) {
            out.writeString(s);
        }
        tableIdent.writeTo(out);
    }
}
