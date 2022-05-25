/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.analyze.expressions;

import io.crate.analyze.relations.FieldProvider;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.SimpleReference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class TableReferenceResolver implements FieldProvider<SimpleReference> {

    private final Collection<SimpleReference> tableReferences;
    private final RelationName relationName;
    private final List<SimpleReference> references = new ArrayList<>();

    public TableReferenceResolver(Collection<SimpleReference> tableReferences, RelationName relationName) {
        this.tableReferences = tableReferences;
        this.relationName = relationName;
    }

    @Override
    public SimpleReference resolveField(QualifiedName qualifiedName,
                                  @Nullable List<String> path,
                                  Operation operation,
                                  boolean errorOnUnknownObjectKey) {
        ColumnIdent columnIdent = ColumnIdent.fromNameSafe(qualifiedName, path);
        for (SimpleReference reference : tableReferences) {
            if (reference.column().equals(columnIdent)) {
                if (reference instanceof GeneratedReference) {
                    throw new IllegalArgumentException("A generated column cannot be based on a generated column");
                }
                references.add(reference);
                return reference;
            }
        }

        throw new ColumnUnknownException(columnIdent.sqlFqn(), relationName);
    }

    public List<SimpleReference> references() {
        return references;
    }
}
