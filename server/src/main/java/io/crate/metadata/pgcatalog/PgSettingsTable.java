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

import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.INTEGER;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.STRING_ARRAY;

import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.metadata.settings.session.NamedSessionSetting;


public final class PgSettingsTable {

    public static final RelationName IDENT = new RelationName(PgCatalogSchemaInfo.NAME, "pg_settings");

    private PgSettingsTable() {}

    public static SystemTable<NamedSessionSetting> INSTANCE = SystemTable.<NamedSessionSetting>builder(IDENT)
        .add("name", STRING, NamedSessionSetting::name)
        .add("setting", STRING, NamedSessionSetting::value)
        .add("unit", STRING, c -> null)
        .add("category", STRING, c -> null)
        .add("short_desc", STRING, NamedSessionSetting::description)
        .add("extra_desc", STRING, c -> null)
        .add("context", STRING, c -> null)
        .add("vartype", STRING, c -> c.type().getName())
        .add("source", STRING, c -> null)
        .add("enumvals", STRING_ARRAY, c -> null)
        .add("min_val", STRING, c -> null)
        .add("max_val", STRING, c -> null)
        .add("boot_val", STRING, NamedSessionSetting::defaultValue)
        .add("reset_val", STRING, NamedSessionSetting::defaultValue)
        .add("sourcefile", STRING, c -> null)
        .add("sourceline", INTEGER, c -> null)
        .add("pending_restart", BOOLEAN, c -> null)
        .build();
}
