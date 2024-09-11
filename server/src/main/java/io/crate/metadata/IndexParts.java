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

import java.util.List;

import org.jetbrains.annotations.Nullable;

import io.crate.execution.ddl.tables.AlterTableOperation;

/**
 * Parts of an encoded index: Schema, table and partitionIdent.
 * Use {@link IndexName#decode(String)} to create instances from an indexName.
 */
public record IndexParts(String schema, String table, @Nullable String partitionIdent) {

    public static final List<String> DANGLING_INDICES_PREFIX_PATTERNS = List.of(
        AlterTableOperation.RESIZE_PREFIX + "*"
    );

    public String partitionIdent() {
        return isPartitioned() ? partitionIdent : "";
    }

    public boolean isPartitioned() {
        return partitionIdent != null;
    }

    public RelationName toRelationName() {
        return new RelationName(schema, table);
    }

    public String toFullyQualifiedName() {
        return schema + "." + table;
    }
}
