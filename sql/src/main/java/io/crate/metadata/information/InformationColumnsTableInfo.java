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
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;

public class InformationColumnsTableInfo extends InformationTableInfo {

    public static final String NAME = "columns";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent COLUMN_NAME = new ColumnIdent("column_name");
        public static final ColumnIdent ORDINAL_POSITION = new ColumnIdent("ordinal_position");
        public static final ColumnIdent DATA_TYPE = new ColumnIdent("data_type");
        public static final ColumnIdent IS_GENERATED = new ColumnIdent("is_generated");
        public static final ColumnIdent GENERATION_EXPRESSION = new ColumnIdent("generation_expression");
    }

    public static class ReferenceInfos {
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final ReferenceInfo TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING);
        public static final ReferenceInfo COLUMN_NAME = info(Columns.COLUMN_NAME, DataTypes.STRING);
        public static final ReferenceInfo ORDINAL_POSITION = info(Columns.ORDINAL_POSITION, DataTypes.SHORT);
        public static final ReferenceInfo DATA_TYPE = info(Columns.DATA_TYPE, DataTypes.STRING);
        public static final ReferenceInfo IS_GENERATED = info(Columns.IS_GENERATED, DataTypes.BOOLEAN);
        public static final ReferenceInfo GENERATION_EXPRESSION = info(Columns.GENERATION_EXPRESSION, DataTypes.STRING);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationColumnsTableInfo(ClusterService clusterService) {
        super(clusterService,
                IDENT,
                ImmutableList.of(Columns.SCHEMA_NAME, Columns.TABLE_NAME, Columns.COLUMN_NAME),
                ImmutableSortedMap.<ColumnIdent, ReferenceInfo>naturalOrder()
                    .put(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME)
                    .put(Columns.TABLE_NAME, ReferenceInfos.TABLE_NAME)
                    .put(Columns.COLUMN_NAME, ReferenceInfos.COLUMN_NAME)
                    .put(Columns.ORDINAL_POSITION, ReferenceInfos.ORDINAL_POSITION)
                    .put(Columns.DATA_TYPE, ReferenceInfos.DATA_TYPE)
                    .put(Columns.IS_GENERATED, ReferenceInfos.IS_GENERATED)
                    .put(Columns.GENERATION_EXPRESSION, ReferenceInfos.GENERATION_EXPRESSION)
                    .build()
        );
    }
}
