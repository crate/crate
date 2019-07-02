/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.metadata.information;

import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.view.ViewInfo;

import java.util.Map;

import static io.crate.execution.engine.collect.NestableCollectExpression.forFunction;
import static io.crate.types.DataTypes.BOOLEAN;
import static io.crate.types.DataTypes.STRING;

public class InformationViewsTableInfo extends InformationTableInfo<ViewInfo> {

    public static final String NAME = "views";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final String CHECK_OPTION_NONE = "NONE";

    private static ColumnRegistrar<ViewInfo> columnRegistrar() {
        return new ColumnRegistrar<ViewInfo>(IDENT, RowGranularity.DOC)
            .register("table_catalog", STRING, () -> forFunction(r -> r.ident().schema()))
            .register("table_schema", STRING, () -> forFunction(r -> r.ident().schema()))
            .register("table_name", STRING, () -> forFunction(r -> r.ident().name()))
            .register("view_definition", STRING, () -> forFunction(ViewInfo::definition))
            .register("check_option", STRING, () -> forFunction(r -> CHECK_OPTION_NONE))
            .register("is_updatable", BOOLEAN, () -> forFunction(r -> false))
            .register("owner", STRING, () -> forFunction(ViewInfo::owner));
    }

    static Map<ColumnIdent, RowCollectExpressionFactory<ViewInfo>> expressions() {
        return columnRegistrar().expressions();
    }

    InformationViewsTableInfo() {
        super(IDENT, columnRegistrar(), "table_catalog", "table_name", "table_schema"
        );
    }
}
