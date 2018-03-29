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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RoutineInfo;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.types.DataTypes;

import java.util.Map;

public class InformationRoutinesTableInfo extends InformationTableInfo {

    public static final String NAME = "routines";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent SPECIFIC_NAME = new ColumnIdent("specific_name");
        static final ColumnIdent ROUTINE_NAME = new ColumnIdent("routine_name");
        static final ColumnIdent ROUTINE_TYPE = new ColumnIdent("routine_type");
        static final ColumnIdent ROUTINE_SCHEMA = new ColumnIdent("routine_schema");
        static final ColumnIdent ROUTINE_BODY = new ColumnIdent("routine_body");
        static final ColumnIdent ROUTINE_DEFINITION = new ColumnIdent("routine_definition");
        static final ColumnIdent DATA_TYPE = new ColumnIdent("data_type");
        static final ColumnIdent IS_DETERMINISTIC = new ColumnIdent("is_deterministic");
    }

    private static ColumnRegistrar columnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.ROUTINE_NAME, DataTypes.STRING)
            .register(Columns.ROUTINE_TYPE, DataTypes.STRING)
            .register(Columns.ROUTINE_SCHEMA, DataTypes.STRING)
            .register(Columns.SPECIFIC_NAME, DataTypes.STRING)
            .register(Columns.ROUTINE_BODY, DataTypes.STRING)
            .register(Columns.ROUTINE_DEFINITION, DataTypes.STRING)
            .register(Columns.DATA_TYPE, DataTypes.STRING)
            .register(Columns.IS_DETERMINISTIC, DataTypes.BOOLEAN);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<RoutineInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<RoutineInfo>>builder()
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::name))
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::type))
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::schema))
            .put(InformationRoutinesTableInfo.Columns.SPECIFIC_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::specificName))
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_BODY,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::body))
            .put(InformationRoutinesTableInfo.Columns.ROUTINE_DEFINITION,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::definition))
            .put(InformationRoutinesTableInfo.Columns.DATA_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(RoutineInfo::dataType))
            .put(InformationRoutinesTableInfo.Columns.IS_DETERMINISTIC,
                () -> RowContextCollectorExpression.forFunction(RoutineInfo::isDeterministic))
            .build();
    }

    InformationRoutinesTableInfo() {
        super(
            IDENT,
            columnRegistrar(),
            ImmutableList.of()
        );
    }
}
