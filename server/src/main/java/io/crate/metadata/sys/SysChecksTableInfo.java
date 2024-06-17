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

package io.crate.metadata.sys;

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;

import io.crate.expression.reference.sys.check.SysCheck;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public final class SysChecksTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "checks");

    public static SystemTable<SysCheck> INSTANCE = SystemTable.<SysCheck>builder(IDENT)
        .add("id", INTEGER, SysCheck::id)
        .add("severity", INTEGER, x -> x.severity().value())
        .add("description", STRING, SysCheck::description)
        .add("passed", BOOLEAN, SysCheck::isValid)
        .setPrimaryKeys(new ColumnIdent("id"))
        .build();
}
