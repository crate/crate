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

import io.crate.metadata.RelationName;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.SystemTable;

public class InformationRoutinesTableInfo {

    public static final String NAME = "routines";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static SystemTable<RoutineInfo> INSTANCE = SystemTable.<RoutineInfo>builder(IDENT)
        .add("routine_name", STRING, RoutineInfo::name)
        .add("routine_type", STRING, RoutineInfo::type)
        .add("routine_schema", STRING, RoutineInfo::schema)
        .add("specific_name", STRING, RoutineInfo::specificName)
        .add("routine_body", STRING, RoutineInfo::body)
        .add("routine_definition", STRING, RoutineInfo::definition)
        .add("data_type", STRING, RoutineInfo::dataType)
        .add("is_deterministic", BOOLEAN, RoutineInfo::isDeterministic)
        .build();
}
