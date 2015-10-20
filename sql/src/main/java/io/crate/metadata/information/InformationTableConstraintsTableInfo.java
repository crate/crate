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
import io.crate.metadata.*;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.ClusterService;

import java.util.LinkedHashMap;

public class InformationTableConstraintsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_constraints";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        public static final ColumnIdent CONSTRAINT_TYPE = new ColumnIdent("constraint_type");
    }

    public static class ReferenceInfos {
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
        public static final ReferenceInfo TABLE_NAME = info(Columns.TABLE_NAME, DataTypes.STRING);
        public static final ReferenceInfo CONSTRAINT_NAME = info(Columns.CONSTRAINT_NAME, new ArrayType(DataTypes.STRING));
        public static final ReferenceInfo CONSTRAINT_TYPE = info(Columns.CONSTRAINT_TYPE, DataTypes.STRING);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationTableConstraintsTableInfo(InformationSchemaInfo schemaInfo, ClusterService clusterService) {
        super(schemaInfo,
              clusterService,
                IDENT,
                ImmutableList.<ColumnIdent>of(),
                new LinkedHashMap<ColumnIdent, ReferenceInfo>() {{
                    put(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME);
                    put(Columns.TABLE_NAME, ReferenceInfos.TABLE_NAME);
                    put(Columns.CONSTRAINT_NAME, ReferenceInfos.CONSTRAINT_NAME);
                    put(Columns.CONSTRAINT_TYPE, ReferenceInfos.CONSTRAINT_TYPE);
                }}
        );
    }
}
