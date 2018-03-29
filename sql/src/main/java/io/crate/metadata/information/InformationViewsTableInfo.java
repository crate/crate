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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ColumnRegistrar;
import io.crate.metadata.view.ViewInfo;
import io.crate.types.DataTypes;
import org.apache.lucene.util.BytesRef;

import java.util.Map;

public class InformationViewsTableInfo extends InformationTableInfo {

    public static final String NAME = "views";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    private static final BytesRef CHECK_OPTION_NONE = new BytesRef("NONE");

    private static class Columns {
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        static final ColumnIdent VIEW_DEFINITION = new ColumnIdent("view_definition");
        static final ColumnIdent CHECK_OPTION = new ColumnIdent("check_option");
        static final ColumnIdent IS_UPDATABLE = new ColumnIdent("is_updatable");
    }

    private static ColumnRegistrar buildColumnRegistrar() {
        return new ColumnRegistrar(IDENT, RowGranularity.DOC)
            .register(Columns.TABLE_CATALOG, DataTypes.STRING)
            .register(Columns.TABLE_SCHEMA, DataTypes.STRING)
            .register(Columns.TABLE_NAME, DataTypes.STRING)
            .register(Columns.VIEW_DEFINITION, DataTypes.STRING)
            .register(Columns.CHECK_OPTION, DataTypes.STRING)
            .register(Columns.IS_UPDATABLE, DataTypes.BOOLEAN);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ViewInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ViewInfo>>builder()
            .put(Columns.TABLE_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().schema()))
            .put(Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.ident().name()))
            .put(Columns.VIEW_DEFINITION,
                () -> RowContextCollectorExpression.objToBytesRef(ViewInfo::definition))
            .put(Columns.CHECK_OPTION,
                () -> RowContextCollectorExpression.forFunction(r -> CHECK_OPTION_NONE))
            .put(Columns.IS_UPDATABLE,
                () -> RowContextCollectorExpression.forFunction(r -> false))
            .build();
    }

    InformationViewsTableInfo() {
        super(
            IDENT,
            buildColumnRegistrar(),
            ImmutableList.of(Columns.TABLE_CATALOG, Columns.TABLE_SCHEMA, Columns.TABLE_NAME)
        );
    }
}
