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

import io.crate.Constants;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;
import io.crate.types.DataTypes;

public class UserMappingsTableInfo {

    public static final String NAME = "user_mappings";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public record UserMapping(String userName, String serverName) {
    }

    public static SystemTable<UserMapping> create() {
        return SystemTable.<UserMapping>builder(IDENT)
            .add("authorization_identifier", DataTypes.STRING, UserMapping::userName)
            .add("foreign_server_catalog", DataTypes.STRING, ignored -> Constants.DB_NAME)
            .add("foreign_server_name", DataTypes.STRING, UserMapping::serverName)
            .build();
    }
}
