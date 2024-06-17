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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;

import io.crate.Constants;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.view.ViewInfo;

public class InformationViewsTableInfo {

    public static final String NAME = "views";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String CHECK_OPTION_NONE = "NONE";

    public static SystemTable<ViewInfo> INSTANCE = SystemTable.<ViewInfo>builder(IDENT)
        .add("table_catalog", STRING, r -> Constants.DB_NAME)
        .add("table_schema", STRING, r -> r.ident().schema())
        .add("table_name", STRING, r -> r.ident().name())
        .add("view_definition", STRING, ViewInfo::definition)
        .add("check_option", STRING, r -> CHECK_OPTION_NONE)
        .add("is_updatable", BOOLEAN, r -> false)
        .add("owner", STRING, ViewInfo::owner)
        .setPrimaryKeys(
            new ColumnIdent("table_catalog"),
            new ColumnIdent("table_name"),
            new ColumnIdent("table_schema")
        )
        .build();
}
