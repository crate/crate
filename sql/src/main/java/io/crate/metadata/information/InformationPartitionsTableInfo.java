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
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.LinkedHashMap;

public class InformationPartitionsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_partitions";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent PARTITION_IDENT = new ColumnIdent("partition_ident");
        public static final ColumnIdent VALUES = new ColumnIdent("values");
        public static final ColumnIdent NUMBER_OF_SHARDS = new ColumnIdent("number_of_shards");
        public static final ColumnIdent NUMBER_OF_REPLICAS = new ColumnIdent("number_of_replicas");
    }

    public static class ReferenceInfos {
        public static final ReferenceInfo TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING);
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final ReferenceInfo PARTITION_IDENT = info(Columns.PARTITION_IDENT, DataTypes.STRING);
        public static final ReferenceInfo VALUES = info(Columns.VALUES, DataTypes.OBJECT);
        public static final ReferenceInfo NUMBER_OF_SHARDS = info(Columns.NUMBER_OF_SHARDS, DataTypes.INTEGER);
        public static final ReferenceInfo NUMBER_OF_REPLICAS = info(Columns.NUMBER_OF_REPLICAS, DataTypes.STRING);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationPartitionsTableInfo(InformationSchemaInfo schemaInfo) {
        super(schemaInfo,
                IDENT,
                ImmutableList.<ColumnIdent>of(),
                new LinkedHashMap<ColumnIdent, ReferenceInfo>() {{
                    put(Columns.TABLE_NAME, ReferenceInfos.TABLE_NAME);
                    put(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME);
                    put(Columns.PARTITION_IDENT, ReferenceInfos.PARTITION_IDENT);
                    put(Columns.VALUES, ReferenceInfos.VALUES);
                    put(Columns.NUMBER_OF_SHARDS, ReferenceInfos.NUMBER_OF_SHARDS);
                    put(Columns.NUMBER_OF_REPLICAS, ReferenceInfos.NUMBER_OF_REPLICAS);
                }}
        );
    }
}
