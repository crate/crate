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
import com.google.common.collect.ImmutableSortedMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.metadata.table.ConstraintInfo;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;

public class InformationTableConstraintsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_constraints";
    public static final RelationName IDENT = new RelationName(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent CONSTRAINT_CATALOG = new ColumnIdent("constraint_catalog");
        static final ColumnIdent CONSTRAINT_SCHEMA = new ColumnIdent("constraint_schema");
        static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        static final ColumnIdent CONSTRAINT_TYPE = new ColumnIdent("constraint_type");
        static final ColumnIdent IS_DEFERRABLE = new ColumnIdent("is_deferrable");
        static final ColumnIdent INITIALLY_DEFERRED = new ColumnIdent("initially_deferred");
    }

    public static class References {
        static final Reference CONSTRAINT_CATALOG = createRef(Columns.CONSTRAINT_CATALOG, DataTypes.STRING);
        static final Reference CONSTRAINT_SCHEMA = createRef(Columns.CONSTRAINT_SCHEMA, DataTypes.STRING);
        static final Reference CONSTRAINT_NAME = createRef(Columns.CONSTRAINT_NAME, DataTypes.STRING);
        static final Reference TABLE_CATALOG = createRef(Columns.TABLE_CATALOG, DataTypes.STRING);
        static final Reference TABLE_SCHEMA = createRef(Columns.TABLE_SCHEMA, DataTypes.STRING);
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        static final Reference CONSTRAINT_TYPE = createRef(Columns.CONSTRAINT_TYPE, DataTypes.STRING);
        static final Reference IS_DEFERRABLE = createRef(Columns.IS_DEFERRABLE, DataTypes.STRING);
        static final Reference INITIALLY_DEFERRED = createRef(Columns.INITIALLY_DEFERRED, DataTypes.STRING);
    }

    public static Map<ColumnIdent, RowCollectExpressionFactory<ConstraintInfo>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<ConstraintInfo>>builder()
            .put(Columns.CONSTRAINT_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.tableIdent().schema()))
            .put(Columns.CONSTRAINT_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.tableIdent().schema()))
            .put(Columns.CONSTRAINT_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.constraintName()))
            .put(Columns.TABLE_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.tableIdent().schema()))
            .put(Columns.TABLE_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.tableIdent().schema()))
            .put(Columns.TABLE_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.tableIdent().name()))
            .put(Columns.CONSTRAINT_TYPE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> r.constraintType()))
            .put(Columns.IS_DEFERRABLE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> "NO"))
            .put(Columns.INITIALLY_DEFERRED,
                () -> RowContextCollectorExpression.objToBytesRef(r -> "NO"))
            .build();
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    InformationTableConstraintsTableInfo() {
        super(
            IDENT,
            ImmutableList.of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.CONSTRAINT_CATALOG, References.CONSTRAINT_CATALOG)
                .put(Columns.CONSTRAINT_SCHEMA, References.CONSTRAINT_SCHEMA)
                .put(Columns.CONSTRAINT_NAME, References.CONSTRAINT_NAME)
                .put(Columns.TABLE_CATALOG, References.TABLE_CATALOG)
                .put(Columns.TABLE_SCHEMA, References.TABLE_SCHEMA)
                .put(Columns.TABLE_NAME, References.TABLE_NAME)
                .put(Columns.CONSTRAINT_TYPE, References.CONSTRAINT_TYPE)
                .put(Columns.IS_DEFERRABLE, References.IS_DEFERRABLE)
                .put(Columns.INITIALLY_DEFERRED, References.INITIALLY_DEFERRED)
                .build()
        );
    }
}
