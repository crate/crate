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

import io.crate.expression.roles.RoleTableGrant;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public class InformationRoleTableGrantsTableInfo {
    public static final String NAME = "role_table_grants";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<RoleTableGrant> INSTANCE = SystemTable.<RoleTableGrant>builder(IDENT)
        .add("grantor", DataTypes.STRING, RoleTableGrant::grantor)
        .add("grantee", DataTypes.STRING, RoleTableGrant::grantee)
        .add("table_catalog", DataTypes.STRING, RoleTableGrant::tableCatalog)
        .add("table_schema", DataTypes.STRING, RoleTableGrant::tableSchema)
        .add("table_name", DataTypes.STRING, RoleTableGrant::tableName)
        .add("privilege_type", DataTypes.STRING, RoleTableGrant::privilegeType)
        .add("is_grantable", DataTypes.BOOLEAN, RoleTableGrant::isGrantable)
        .add("with_hierarchy", DataTypes.BOOLEAN, RoleTableGrant::withHierarchy)
        .build();
}
