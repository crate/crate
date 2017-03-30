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

public class InformationRoutinesTableInfo extends InformationTableInfo {

    public static final String NAME = "routines";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent ROUTINE_NAME = new ColumnIdent("routine_name");
        public static final ColumnIdent ROUTINE_TYPE = new ColumnIdent("routine_type");
        public static final ColumnIdent ROUTINE_SCHEMA = new ColumnIdent("routine_schema");
        public static final ColumnIdent ROUTINE_BODY = new ColumnIdent("routine_body");
        public static final ColumnIdent ROUTINE_DEFINITION = new ColumnIdent("routine_definition");
        public static final ColumnIdent DATA_TYPE = new ColumnIdent("data_type");
        public static final ColumnIdent IS_DETERMINISTIC = new ColumnIdent("is_deterministic");
    }

    public static class References {
        public static final Reference ROUTINE_NAME = info(Columns.ROUTINE_NAME, DataTypes.STRING);
        public static final Reference ROUTINE_TYPE = info(Columns.ROUTINE_TYPE, DataTypes.STRING);
        public static final Reference ROUTINE_SCHEMA = info(Columns.ROUTINE_SCHEMA, DataTypes.STRING);
        public static final Reference ROUTINE_BODY = info(Columns.ROUTINE_BODY, DataTypes.STRING);
        public static final Reference ROUTINE_DEFINITION = info(Columns.ROUTINE_DEFINITION, DataTypes.STRING);
        public static final Reference DATA_TYPE = info(Columns.DATA_TYPE, DataTypes.STRING);
        public static final Reference IS_DETERMINISTIC = info(Columns.IS_DETERMINISTIC, DataTypes.BOOLEAN);
    }

    private static Reference info(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationRoutinesTableInfo(ClusterService clusterService) {
        super(clusterService,
            IDENT,
            ImmutableList.<ColumnIdent>of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.ROUTINE_NAME, References.ROUTINE_NAME)
                .put(Columns.ROUTINE_TYPE, References.ROUTINE_TYPE)
                .put(Columns.ROUTINE_SCHEMA, References.ROUTINE_SCHEMA)
                .put(Columns.ROUTINE_BODY, References.ROUTINE_BODY)
                .put(Columns.ROUTINE_DEFINITION, References.ROUTINE_DEFINITION)
                .put(Columns.DATA_TYPE, References.DATA_TYPE)
                .put(Columns.IS_DETERMINISTIC, References.IS_DETERMINISTIC)
                .build()
        );
    }
}
