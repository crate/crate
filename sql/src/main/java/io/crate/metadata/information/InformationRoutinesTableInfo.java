/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;

import java.util.Map;
import static io.crate.types.DataTypes.STRING;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;

public class InformationRoutinesTableInfo extends InformationTableInfo<RoutineInfo> {

    public static final String NAME = "routines";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static ColumnRegistrar<RoutineInfo> columnRegistrar() {
        return new ColumnRegistrar<RoutineInfo>(IDENT, RowGranularity.DOC)
            .register("routine_name", STRING, () -> forFunction(RoutineInfo::name))
            .register("routine_type", STRING, () -> forFunction(RoutineInfo::type))
            .register("routine_schema", STRING, () -> forFunction(RoutineInfo::schema))
            .register("specific_name", STRING, () -> forFunction(RoutineInfo::specificName))
            .register("routine_body", STRING, () -> forFunction(RoutineInfo::body))
            .register("routine_definition", STRING, () -> forFunction(RoutineInfo::definition))
            .register("data_type", STRING, () -> forFunction(RoutineInfo::dataType))
            .register("is_deterministic", BOOLEAN, () -> forFunction(RoutineInfo::isDeterministic));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<RoutineInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    InformationRoutinesTableInfo() {
        super(IDENT, columnRegistrar());
    }
}
