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

package io.crate.expression.symbol;

import io.crate.analyze.QuerySpec;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.metadata.Path;
import io.crate.types.DataType;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * A Field is a expression which refers to a column of a (virtual) table.
 *
 * <pre>
 *     Example:
 *                   TableRelation[t1]
 *                   |
 *     select x from t1
 *     |      |
 *     |      Field         <---- this is inside {@link QuerySpec#outputs()}
 *     |        path: x
 *     |        relation: TableRelation[t1]
 *     |
 *     |
 *     QueriedTable[t1]
 *      fields(): [
 *          Field
 *            path: x
 *            relation: QueriedTable
 *      ]
 * </pre>
 *
 * Note:
 *  Since the relation of a Field can be a virtual table,
 *  a Field can also represent the computation of a scalar or aggregation
 */
public class Field extends Symbol {

    private final AnalyzedRelation relation;
    private final Path path;
    private final DataType valueType;

    public Field(AnalyzedRelation relation, Path path, DataType valueType) {
        assert path != null : "path must not be null";
        assert relation != null : "relation must not be null";

        this.relation = relation;
        this.path = path;
        this.valueType = valueType;
    }

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
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Field is not streamable");
    }

    @Override
    public String toString() {
        return representation();
    }

    @Override
    public String representation() {
        return "Field{" + relation + "." + path +
               ", type=" + valueType +
               '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Field that = (Field) o;

        if (!relation.equals(that.relation)) return false;
        if (!path.equals(that.path)) return false;
        if (!valueType.equals(that.valueType)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = Objects.hashCode(relation.getQualifiedName());
        result = 31 * result + path.hashCode();
        result = 31 * result + valueType.hashCode();
        return result;
    }

    /**
     * @return the position of the field in its relation
     */
    public int index() {
        int idx = relation.fields().indexOf(this);
        assert idx >= 0 : "idx must be >= 0";
        return idx;
    }
}
