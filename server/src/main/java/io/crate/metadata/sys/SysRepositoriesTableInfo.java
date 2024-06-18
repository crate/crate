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

import static io.crate.types.DataTypes.STRING;

import java.util.List;
import java.util.stream.Collectors;

import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.repositories.Repository;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.SystemTable;

public class SysRepositoriesTableInfo {

    public static final RelationName IDENT = new RelationName(SysSchemaInfo.NAME, "repositories");

    public static SystemTable<Repository> create(List<Setting<?>> maskedSettings) {
        var maskedSettingNames = maskedSettings.stream().map(Setting::getKey).collect(Collectors.toSet());
        return SystemTable.<Repository>builder(IDENT)
            .add("name", STRING, (Repository r) -> r.getMetadata().name())
            .add("type", STRING, (Repository r) -> r.getMetadata().type())
            .addDynamicObject("settings", STRING, r -> r.getMetadata().settings().getAsStructuredMap(maskedSettingNames))
            .setPrimaryKeys(ColumnIdent.of("name"))
            .withRouting((state, routingProvider, sessionSettings) -> routingProvider.forRandomMasterOrDataNode(IDENT, state.nodes()))
            .build();
    }
}
