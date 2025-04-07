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

package io.crate.metadata;

import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.elasticsearch.common.settings.Settings;
import org.jetbrains.annotations.Nullable;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CheckConstraint;

/**
 * Base interface for tables ({@link io.crate.metadata.table.TableInfo}) and views ({@link io.crate.metadata.view.ViewInfo}).
 */
public interface RelationInfo extends Iterable<Reference> {

    String PK_SUFFIX = "_pkey";

    enum RelationType {
        BASE_TABLE("BASE TABLE"),
        VIEW("VIEW"),
        FOREIGN("FOREIGN");

        private final String prettyName;

        RelationType(String prettyName) {
            this.prettyName = prettyName;
        }

        public String pretty() {
            return this.prettyName;
        }
    }

    /**
     * Returns the root columns of this table with predictable order.
     * Excludes system columns.
     * <p>
     * Use {@link #iterator()} to get root, child and system columns.
     * </p>
     * <p>
     * Use {@link DocTableInfo#allReferences()} to get both active and dropped
     * columns for all:
     * root, child, system and index columns.
     * </p>
     */
    Collection<Reference> rootColumns();

    default Collection<Reference> droppedColumns() {
        return List.of();
    }

    default int maxPosition() {
        return rootColumns().stream()
            .filter(ref -> !ref.column().isSystemColumn())
            .mapToInt(Reference::position)
            .max()
            .orElse(0);
    }

    RowGranularity rowGranularity();

    RelationName ident();

    @Nullable
    default String pkConstraintName() {
        return null;
    }

    @Nullable
    default String pkConstraintNameOrDefault() {
        String pkName = pkConstraintName();
        if (pkName != null) {
            return pkName;
        }
        if (primaryKey().isEmpty() || (primaryKey().size() == 1 && primaryKey().get(0).isSystemColumn())) {
            return null;
        } else {
            return ident().name() + PK_SUFFIX;
        }
    }

    List<ColumnIdent> primaryKey();

    default List<CheckConstraint<Symbol>> checkConstraints() {
        return List.of();
    }

    Settings parameters();

    Set<Operation> supportedOperations();

    RelationType relationType();
}
