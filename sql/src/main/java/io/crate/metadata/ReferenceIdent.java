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

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.List;
import java.util.Locale;

public class ReferenceIdent {

    private final RelationName relationName;
    private final ColumnIdent columnIdent;

    public ReferenceIdent(StreamInput in) throws IOException {
        columnIdent = new ColumnIdent(in);
        relationName = new RelationName(in);
    }

    public ReferenceIdent(RelationName relationName, ColumnIdent columnIdent) {
        this.relationName = relationName;
        this.columnIdent = columnIdent;
    }

    public ReferenceIdent(RelationName relationName, String column) {
        this(relationName, new ColumnIdent(column));
    }

    public ReferenceIdent(RelationName relationName, String column, @Nullable List<String> path) {
        this(relationName, new ColumnIdent(column, path));
    }

    public RelationName tableIdent() {
        return relationName;
    }

    public ColumnIdent columnIdent() {
        return columnIdent;
    }

    public ReferenceIdent columnReferenceIdent() {
        if (columnIdent.isTopLevel()) {
            return this;
        }
        return new ReferenceIdent(relationName, columnIdent.name());
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
               Objects.equal(relationName, o.relationName);
    }

    @Override
    public int hashCode() {
        int result = relationName.hashCode();
        result = 31 * result + columnIdent.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return String.format(Locale.ENGLISH, "<RefIdent: %s->%s>", relationName, columnIdent);
    }

    public void writeTo(StreamOutput out) throws IOException {
        columnIdent.writeTo(out);
        relationName.writeTo(out);
    }
}
