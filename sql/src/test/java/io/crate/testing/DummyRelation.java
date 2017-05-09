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

package io.crate.testing;

import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.AnalyzedRelationVisitor;
import io.crate.analyze.symbol.Field;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Path;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;

import javax.annotation.Nonnull;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * relation that will return a Reference with Doc granularity / String type for all columns
 */
public class DummyRelation implements AnalyzedRelation {

    private final Set<ColumnIdent> columnReferences = new HashSet<>();
    private QualifiedName name = new QualifiedName("dummy");

    public DummyRelation(String... referenceNames) {
        for (String referenceName : referenceNames) {
            columnReferences.add(ColumnIdent.fromPath(referenceName));
        }
    }

    @Override
    public <C, R> R accept(AnalyzedRelationVisitor<C, R> visitor, C context) {
        return null;
    }

    @Override
    public Field getField(Path path, Operation operation) throws UnsupportedOperationException {
        if (columnReferences.contains(path)) {
            return new Field(this, path, DataTypes.STRING);
        }
        return null;
    }

    @Override
    public List<Field> fields() {
        return null;
    }

    @Override
    public QualifiedName getQualifiedName() {
        return name;
    }

    @Override
    public void setQualifiedName(@Nonnull QualifiedName qualifiedName) {
        this.name = qualifiedName;
    }
}
