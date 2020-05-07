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

package io.crate.analyze.expressions;

import io.crate.analyze.relations.FieldProvider;
import io.crate.exceptions.ColumnUnknownException;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.QualifiedName;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;

public class TableReferenceResolver implements FieldProvider<Reference> {

    private final Collection<Reference> tableReferences;
    private final RelationName relationName;
    private final List<Reference> references = new ArrayList<>();

    public TableReferenceResolver(Collection<Reference> tableReferences, RelationName relationName) {
        this.tableReferences = tableReferences;
        this.relationName = relationName;
    }

    public static ColumnIdent columnIdent(QualifiedName qualifiedName, @Nullable List<String> path) {
        List<String> qualifiedNameParts = qualifiedName.getParts();
        if (qualifiedNameParts.size() != 1) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Column reference \"%s\" has too many parts. " +
                "A column must not have a schema or a table here.", qualifiedName));
        }
        return new ColumnIdent(qualifiedNameParts.get(qualifiedNameParts.size() - 1), path);
    }

    @Override
    public Reference resolveField(QualifiedName qualifiedName, @Nullable List<String> path, Operation operation) {
        ColumnIdent columnIdent = columnIdent(qualifiedName, path);
        for (Reference reference : tableReferences) {
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

    public List<Reference> references() {
        return references;
    }
}
