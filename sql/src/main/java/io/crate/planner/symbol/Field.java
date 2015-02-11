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

package io.crate.planner.symbol;

import com.google.common.base.MoreObjects;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.Path;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class Field extends Symbol {

    public static final SymbolFactory<Field> FACTORY = new SymbolFactory<Field>() {
        @Override
        public Field newInstance() {
            return new Field();
        }
    };

    private AnalyzedRelation relation;
    private Path path;
    private DataType valueType;

    public Field(AnalyzedRelation relation, Path path, DataType valueType) {
        this.relation = relation;
        this.path = path;
        this.valueType = valueType;
    }

    private Field() {}

    public Path path() {
        return path;
    }

    public AnalyzedRelation relation() {
        return relation;
    }

    @Override
    public SymbolType symbolType() {
        return SymbolType.RELATION_OUTPUT;
    }

    @Override
    public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
        return visitor.visitField(this, context);
    }

    @Override
    public DataType valueType() {
        return valueType;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("RelationOutput is not streamable");
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(Field.class)
                .add("path", path)
                .add("valueType", valueType)
                .add("relation", relation).toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field that = (Field) o;

        if (relation != that.relation) return false;
        if (!path.equals(that.path)) return false;
        if (!valueType.equals(that.valueType)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = relation.hashCode();
        result = 31 * result + path.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }
}
