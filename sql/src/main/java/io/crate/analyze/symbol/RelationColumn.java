/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.symbol;

import com.google.common.base.MoreObjects;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class RelationColumn extends InputColumn {

    public static final SymbolFactory<RelationColumn> FACTORY = new SymbolFactory<RelationColumn>() {
        @Override
        public RelationColumn newInstance() {
            return new RelationColumn();
        }
    };

    private QualifiedName relationName;

    public RelationColumn(QualifiedName relationName, int index, @Nullable DataType dataType) {
        super(index, dataType);
        this.relationName = relationName;
    }

    private RelationColumn() {

    }

    public QualifiedName relationName() {
        return relationName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RelationColumn that = (RelationColumn) o;
        if (index() != that.index()) return false;
        return Objects.equals(relationName, that.relationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), relationName);
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.RELATION_COLUMN;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitRelationColumn(this, context);
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        int numParts = in.readVInt();
        List<String> parts = new ArrayList<>();
        for (int i = 0; i < numParts; i++) {
            parts.add(in.readString());
        }
        relationName = new QualifiedName(parts);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(relationName.getParts().size());
        for (String s : relationName.getParts()) {
            out.writeString(s);
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("relationName", relationName)
                .add("index", index())
                .add("type", valueType())
                .toString();
    }
}
