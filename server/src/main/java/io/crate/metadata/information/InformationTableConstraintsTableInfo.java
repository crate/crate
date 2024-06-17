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

package io.crate.metadata.information;

import static io.crate.types.DataTypes.STRING;

import io.crate.Constants;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.table.ConstraintInfo;

public class InformationTableConstraintsTableInfo {

    public static final String NAME = "table_constraints";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<ConstraintInfo> INSTANCE = SystemTable.<ConstraintInfo>builder(IDENT)
        .add("constraint_schema", STRING, r -> r.relationName().schema())
        .add("constraint_name", STRING, ConstraintInfo::constraintName)
        .add("constraint_catalog", STRING, r -> Constants.DB_NAME)
        .add("table_catalog", STRING, r -> Constants.DB_NAME)
        .add("table_schema", STRING, r -> r.relationName().schema())
        .add("table_name", STRING, r -> r.relationName().name())
        .add("constraint_type", STRING, r -> r.constraintType().toString())
        .add("is_deferrable", STRING, ignored -> "NO")
        .add("initially_deferred", STRING, ignored -> "NO")
        .setPrimaryKeys(
            new ColumnIdent("constraint_catalog"),
            new ColumnIdent("constraint_schema"),
            new ColumnIdent("constraint_name")
        )
        .build();
}
