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

package io.crate.metadata.pgcatalog;

import io.crate.action.sql.Cursor;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public final class PgCursors {

    public static final String NAME = "pg_cursors";
    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, NAME);

    private PgCursors() {}

    public static SystemTable<Cursor> INSTANCE = SystemTable.<Cursor>builder(IDENT)
        .add("name", DataTypes.STRING, Cursor::name)
        .add("statement", DataTypes.STRING, Cursor::declareStatement)
        .add("is_holdable", DataTypes.BOOLEAN, Cursor::isHold)
        .add("is_binary", DataTypes.BOOLEAN, Cursor::isBinary)
        .add("is_scrollable", DataTypes.BOOLEAN, Cursor::isScrollable)
        .add("creation_time", DataTypes.TIMESTAMPZ, Cursor::creationTime)
        .build();
}
