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
import io.crate.metadata.RowContextCollectorExpression;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TableIdent;
import io.crate.metadata.expressions.RowCollectExpressionFactory;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class InformationReferentialConstraintsTableInfo extends InformationTableInfo {

    public static final String NAME = "referential_constraints";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        static final ColumnIdent CONSTRAINT_CATALOG = new ColumnIdent("constraint_catalog");
        static final ColumnIdent CONSTRAINT_SCHEMA = new ColumnIdent("constraint_schema");
        static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        static final ColumnIdent UNIQUE_CONSTRAINT_CATALOG = new ColumnIdent("unique_constraint_catalog");
        static final ColumnIdent UNIQUE_CONSTRAINT_SCHEMA = new ColumnIdent("unique_constraint_schema");
        static final ColumnIdent UNIQUE_CONSTRAINT_NAME = new ColumnIdent("unique_constraint_name");
        static final ColumnIdent MATCH_OPTION = new ColumnIdent("match_option");
        static final ColumnIdent UPDATE_RULE = new ColumnIdent("update_rule");
        static final ColumnIdent DELETE_RULE = new ColumnIdent("delete_rule");
    }

    public static class References {
        static final Reference CONSTRAINT_CATALOG = createRef(Columns.CONSTRAINT_CATALOG , DataTypes.STRING);
        static final Reference CONSTRAINT_SCHEMA = createRef(Columns.CONSTRAINT_SCHEMA , DataTypes.STRING);
        static final Reference CONSTRAINT_NAME = createRef(Columns.CONSTRAINT_NAME , DataTypes.STRING);
        static final Reference UNIQUE_CONSTRAINT_CATALOG = createRef(Columns.UNIQUE_CONSTRAINT_CATALOG , DataTypes.STRING);
        static final Reference UNIQUE_CONSTRAINT_SCHEMA = createRef(Columns.UNIQUE_CONSTRAINT_SCHEMA , DataTypes.STRING);
        static final Reference UNIQUE_CONSTRAINT_NAME = createRef(Columns.UNIQUE_CONSTRAINT_NAME , DataTypes.STRING);
        static final Reference MATCH_OPTION = createRef(Columns.MATCH_OPTION , DataTypes.STRING);
        static final Reference UPDATE_RULE = createRef(Columns.UPDATE_RULE , DataTypes.STRING);
        static final Reference DELETE_RULE = createRef(Columns.DELETE_RULE , DataTypes.STRING);
    }

    public static ImmutableMap<ColumnIdent, RowCollectExpressionFactory<Void>> expressions() {
        return ImmutableMap.<ColumnIdent, RowCollectExpressionFactory<Void>>builder()
            .put(Columns.CONSTRAINT_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.CONSTRAINT_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.CONSTRAINT_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_CATALOG,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_SCHEMA,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UNIQUE_CONSTRAINT_NAME,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.MATCH_OPTION,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.UPDATE_RULE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .put(Columns.DELETE_RULE,
                () -> RowContextCollectorExpression.objToBytesRef(r -> null))
            .build();
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    InformationReferentialConstraintsTableInfo() {
        super(
            IDENT,
            ImmutableList.of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.CONSTRAINT_CATALOG, References.CONSTRAINT_CATALOG)
                .put(Columns.CONSTRAINT_SCHEMA, References.CONSTRAINT_SCHEMA)
                .put(Columns.CONSTRAINT_NAME, References.CONSTRAINT_NAME)
                .put(Columns.UNIQUE_CONSTRAINT_CATALOG, References.UNIQUE_CONSTRAINT_CATALOG)
                .put(Columns.UNIQUE_CONSTRAINT_SCHEMA, References.UNIQUE_CONSTRAINT_SCHEMA)
                .put(Columns.UNIQUE_CONSTRAINT_NAME, References.UNIQUE_CONSTRAINT_NAME)
                .put(Columns.MATCH_OPTION, References.MATCH_OPTION)
                .put(Columns.UPDATE_RULE, References.UPDATE_RULE)
                .put(Columns.DELETE_RULE, References.DELETE_RULE)
                .build()
        );
    }
}
