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

package io.crate.metadata;

import io.crate.expression.symbol.Symbol;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.CheckConstraint;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Base interface for tables ({@link io.crate.metadata.table.TableInfo}) and views ({@link io.crate.metadata.view.ViewInfo}).
 */
public interface RelationInfo extends Iterable<Reference> {

    enum RelationType {
        BASE_TABLE("BASE TABLE"),
        VIEW("VIEW");

        private final String prettyName;

        RelationType(String prettyName) {
            this.prettyName = prettyName;
        }

        public String pretty() {
            return this.prettyName;
        }
    }

    /**
     * returns the top level columns of this table with predictable order
     */
    Collection<Reference> columns();

    RowGranularity rowGranularity();

    RelationName ident();

    List<ColumnIdent> primaryKey();

    default List<CheckConstraint<Symbol>> checkConstraints() {
        return List.of();
    }

    Map<String, Object> parameters();

    Set<Operation> supportedOperations();

    RelationType relationType();
}
