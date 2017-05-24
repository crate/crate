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
import com.google.common.collect.ImmutableSortedMap;
import io.crate.metadata.*;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

public class InformationColumnsTableInfo extends InformationTableInfo {

    public static final String NAME = "columns";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent TABLE_CATALOG = new ColumnIdent("table_catalog");
        public static final ColumnIdent COLUMN_NAME = new ColumnIdent("column_name");
        public static final ColumnIdent ORDINAL_POSITION = new ColumnIdent("ordinal_position");
        public static final ColumnIdent DATA_TYPE = new ColumnIdent("data_type");
        public static final ColumnIdent IS_GENERATED = new ColumnIdent("is_generated");
        public static final ColumnIdent IS_NULLABLE = new ColumnIdent("is_nullable");
        public static final ColumnIdent GENERATION_EXPRESSION = new ColumnIdent("generation_expression");
        public static final ColumnIdent COLUMN_DEFAULT = new ColumnIdent("column_default");
        public static final ColumnIdent CHARACTER_MAXIMUM_LENGTH = new ColumnIdent("character_maximum_length");
        public static final ColumnIdent CHARACTER_OCTET_LENGTH = new ColumnIdent("character_octet_length");
        public static final ColumnIdent NUMERIC_PRECISION = new ColumnIdent("numeric_precision");
        public static final ColumnIdent NUMERIC_PRECISION_RADIX = new ColumnIdent("numeric_precision_radix");
        public static final ColumnIdent NUMERIC_SCALE = new ColumnIdent("numeric_scale");
        public static final ColumnIdent DATETIME_PRECISION = new ColumnIdent("datetime_precision");
        public static final ColumnIdent INTERVAL_TYPE = new ColumnIdent("interval_type");
        public static final ColumnIdent INTERVAL_PRECISION = new ColumnIdent("interval_precision");
        public static final ColumnIdent CHARACTER_SET_CATALOG = new ColumnIdent("character_set_catalog");
        public static final ColumnIdent CHARACTER_SET_SCHEMA = new ColumnIdent("character_set_schema");
        public static final ColumnIdent CHARACTER_SET_NAME = new ColumnIdent("character_set_name");
        public static final ColumnIdent COLLATION_CATALOG = new ColumnIdent("collation_catalog");
        public static final ColumnIdent COLLATION_SCHEMA = new ColumnIdent("collation_schema");
        public static final ColumnIdent COLLATION_NAME = new ColumnIdent("collation_name");
        public static final ColumnIdent DOMAIN_CATALOG = new ColumnIdent("domain_catalog");
        public static final ColumnIdent DOMAIN_SCHEMA = new ColumnIdent("domain_schema");
        public static final ColumnIdent DOMAIN_NAME = new ColumnIdent("domain_name");
        public static final ColumnIdent USER_DEFINED_TYPE_CATALOG = new ColumnIdent("user_defined_type_catalog");
        public static final ColumnIdent USER_DEFINED_TYPE_SCHEMA = new ColumnIdent("user_defined_type_schema");
        public static final ColumnIdent USER_DEFINED_TYPE_NAME = new ColumnIdent("user_defined_type_name");
        public static final ColumnIdent CHECK_REFERENCES = new ColumnIdent("check_references");
        public static final ColumnIdent CHECK_ACTION = new ColumnIdent("check_action");
    }

    public static class References {
        public static final Reference TABLE_SCHEMA = info(Columns.TABLE_SCHEMA, DataTypes.STRING, false);
        public static final Reference TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING, false);
        public static final Reference TABLE_CATALOG = info(Columns.TABLE_CATALOG, DataTypes.STRING, false);
        public static final Reference COLUMN_NAME = info(Columns.COLUMN_NAME, DataTypes.STRING, false);
        public static final Reference ORDINAL_POSITION = info(Columns.ORDINAL_POSITION, DataTypes.SHORT, false);
        public static final Reference DATA_TYPE = info(Columns.DATA_TYPE, DataTypes.STRING, false);
        public static final Reference IS_GENERATED = info(Columns.IS_GENERATED, DataTypes.BOOLEAN, false);
        public static final Reference IS_NULLABLE = info(Columns.IS_NULLABLE, DataTypes.BOOLEAN, false);
        public static final Reference GENERATION_EXPRESSION = info(Columns.GENERATION_EXPRESSION, DataTypes.STRING, true);
        public static final Reference COLUMN_DEFAULT = info(Columns.COLUMN_DEFAULT, DataTypes.STRING, true);
        public static final Reference CHARACTER_MAXIMUM_LENGTH = info(Columns.CHARACTER_MAXIMUM_LENGTH, DataTypes.INTEGER, true);
        public static final Reference CHARACTER_OCTET_LENGTH = info(Columns.CHARACTER_OCTET_LENGTH, DataTypes.INTEGER, true);
        public static final Reference NUMERIC_PRECISION = info(Columns.NUMERIC_PRECISION, DataTypes.INTEGER, true);
        public static final Reference NUMERIC_PRECISION_RADIX = info(Columns.NUMERIC_PRECISION_RADIX, DataTypes.INTEGER, true);
        public static final Reference NUMERIC_SCALE = info(Columns.NUMERIC_SCALE, DataTypes.INTEGER, true);
        public static final Reference DATETIME_PRECISION = info(Columns.DATETIME_PRECISION, DataTypes.INTEGER, true);
        public static final Reference INTERVAL_TYPE = info(Columns.INTERVAL_TYPE, DataTypes.STRING, true);
        public static final Reference INTERVAL_PRECISION = info(Columns.INTERVAL_PRECISION, DataTypes.INTEGER, true);
        public static final Reference CHARACTER_SET_CATALOG = info(Columns.CHARACTER_SET_CATALOG, DataTypes.STRING, true);
        public static final Reference CHARACTER_SET_SCHEMA = info(Columns.CHARACTER_SET_SCHEMA, DataTypes.STRING, true);
        public static final Reference CHARACTER_SET_NAME = info(Columns.CHARACTER_SET_NAME, DataTypes.STRING, true);
        public static final Reference COLLATION_CATALOG = info(Columns.COLLATION_CATALOG, DataTypes.STRING, true);
        public static final Reference COLLATION_SCHEMA = info(Columns.COLLATION_SCHEMA, DataTypes.STRING, true);
        public static final Reference COLLATION_NAME = info(Columns.COLLATION_NAME, DataTypes.STRING, true);
        public static final Reference DOMAIN_CATALOG = info(Columns.DOMAIN_CATALOG, DataTypes.STRING, true);
        public static final Reference DOMAIN_SCHEMA = info(Columns.DOMAIN_SCHEMA, DataTypes.STRING, true);
        public static final Reference DOMAIN_NAME = info(Columns.DOMAIN_NAME, DataTypes.STRING, true);
        public static final Reference USER_DEFINED_TYPE_CATALOG = info(Columns.USER_DEFINED_TYPE_CATALOG, DataTypes.STRING, true);
        public static final Reference USER_DEFINED_TYPE_SCHEMA = info(Columns.USER_DEFINED_TYPE_SCHEMA, DataTypes.STRING, true);
        public static final Reference USER_DEFINED_TYPE_NAME = info(Columns.USER_DEFINED_TYPE_NAME, DataTypes.STRING, true);
        public static final Reference CHECK_REFERENCES = info(Columns.CHECK_REFERENCES, DataTypes.STRING, true);
        public static final Reference CHECK_ACTION = info(Columns.CHECK_ACTION, DataTypes.INTEGER, true);
    }

    private static Reference info(ColumnIdent columnIdent, DataType dataType, Boolean nullable) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType, ColumnPolicy.DYNAMIC, Reference.IndexType.NOT_ANALYZED, nullable);
    }

    protected InformationColumnsTableInfo(ClusterService clusterService) {
        super(clusterService,
            IDENT,
            ImmutableList.of(Columns.TABLE_NAME, Columns.TABLE_SCHEMA, Columns.COLUMN_NAME),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.TABLE_SCHEMA, References.TABLE_SCHEMA)
                .put(Columns.TABLE_NAME, References.TABLE_NAME)
                .put(Columns.TABLE_CATALOG, References.TABLE_CATALOG)
                .put(Columns.COLUMN_NAME, References.COLUMN_NAME)
                .put(Columns.ORDINAL_POSITION, References.ORDINAL_POSITION)
                .put(Columns.DATA_TYPE, References.DATA_TYPE)
                .put(Columns.IS_GENERATED, References.IS_GENERATED)
                .put(Columns.IS_NULLABLE, References.IS_NULLABLE)
                .put(Columns.GENERATION_EXPRESSION, References.GENERATION_EXPRESSION)
                .put(Columns.COLUMN_DEFAULT, References.COLUMN_DEFAULT)
                .put(Columns.CHARACTER_MAXIMUM_LENGTH, References.CHARACTER_MAXIMUM_LENGTH)
                .put(Columns.CHARACTER_OCTET_LENGTH, References.CHARACTER_OCTET_LENGTH)
                .put(Columns.NUMERIC_PRECISION, References.NUMERIC_PRECISION)
                .put(Columns.NUMERIC_PRECISION_RADIX, References.NUMERIC_PRECISION_RADIX)
                .put(Columns.NUMERIC_SCALE, References.NUMERIC_SCALE)
                .put(Columns.DATETIME_PRECISION, References.DATETIME_PRECISION)
                .put(Columns.INTERVAL_TYPE, References.INTERVAL_TYPE)
                .put(Columns.INTERVAL_PRECISION, References.INTERVAL_PRECISION)
                .put(Columns.CHARACTER_SET_CATALOG, References.CHARACTER_SET_CATALOG)
                .put(Columns.CHARACTER_SET_SCHEMA, References.CHARACTER_SET_SCHEMA)
                .put(Columns.CHARACTER_SET_NAME, References.CHARACTER_SET_NAME)
                .put(Columns.COLLATION_CATALOG, References.COLLATION_CATALOG)
                .put(Columns.COLLATION_SCHEMA, References.COLLATION_SCHEMA)
                .put(Columns.COLLATION_NAME, References.COLLATION_NAME)
                .put(Columns.DOMAIN_CATALOG, References.DOMAIN_CATALOG)
                .put(Columns.DOMAIN_SCHEMA, References.DOMAIN_SCHEMA)
                .put(Columns.DOMAIN_NAME, References.DOMAIN_NAME)
                .put(Columns.USER_DEFINED_TYPE_CATALOG, References.USER_DEFINED_TYPE_CATALOG)
                .put(Columns.USER_DEFINED_TYPE_SCHEMA, References.USER_DEFINED_TYPE_SCHEMA)
                .put(Columns.USER_DEFINED_TYPE_NAME, References.USER_DEFINED_TYPE_NAME)
                .put(Columns.CHECK_REFERENCES, References.CHECK_REFERENCES)
                .put(Columns.CHECK_ACTION, References.CHECK_ACTION)
                .build()
        );
    }
}
