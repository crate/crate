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

package io.crate.analyze;

import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.relations.JoinPair;
import io.crate.analyze.relations.QueriedRelation;
import io.crate.analyze.symbol.Field;
import io.crate.analyze.symbol.Symbol;
import io.crate.analyze.symbol.Symbols;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.OutputName;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Optional;

public class TwoTableJoin implements QueriedRelation {

    private final QuerySpec querySpec;
    private final QueriedRelation left;
    private final QueriedRelation right;
    private final Optional<OrderBy> remainingOrderBy;
    private final Fields fields;
    private final QualifiedName name;
    private final JoinPair joinPair;

    public TwoTableJoin(QuerySpec querySpec,
                        QueriedRelation left,
                        QueriedRelation right,
                        Optional<OrderBy> remainingOrderBy,
                        JoinPair joinPair) {
        this.querySpec = querySpec;
        this.left = left;
        this.right = right;
        this.name = QualifiedName.of(
            "join",
            left.getQualifiedName().toString(),
            right.getQualifiedName().toString());
        this.remainingOrderBy = remainingOrderBy;
        this.joinPair = joinPair;

        List<Symbol> outputs = querySpec.outputs();
        fields = new Fields(outputs.size());
        for (int i = 0; i < outputs.size(); i++) {
            Symbol output = outputs.get(i);
            String name = Symbols.pathFromSymbol(output).outputName();
            Path fqPath;
            // prefix paths with origin relationName to keep them unique
            if (output instanceof Field) {
                if (left.fields().contains(output)) {
                    fqPath = new ColumnIdent(left.getQualifiedName().toString(), name);
                } else {
                    fqPath = new ColumnIdent(right.getQualifiedName().toString(), name);
                }
            } else {
                fqPath = new OutputName(name);
            }
            fields.add(fqPath, new Field(this, fqPath, output.valueType()));
        }
    }

    public Optional<OrderBy> remainingOrderBy() {
        return remainingOrderBy;
    }

    public QueriedRelation left() {
        return left;
    }

    public QueriedRelation right() {
        return right;
    }

    @Override
    public QuerySpec querySpec() {
        return querySpec;
    }

    public JoinPair joinPair() {
        return joinPair;
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return visitor.visitTwoTableJoin(this, context);
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException, ColumnUnknownException {
        if (operation != Operation.READ) {
            throw new UnsupportedOperationException("getField on TwoTableJoin is only supported for READ operations");
        }
        return fields.get(path);
    }

    @Override
    public List<Field> fields() {
        return fields.asList();
    }

    @Override
    public QualifiedName getQualifiedName() {
        return name;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        throw new UnsupportedOperationException("method not supported");
    }

    @Override
    public String toString() {
        return name.toString();
    }
}
