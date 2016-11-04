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
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.cluster.service.ClusterService;

public class InformationTableConstraintsTableInfo extends InformationTableInfo {

    public static final String NAME = "table_constraints";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent TABLE_SCHEMA = new ColumnIdent("table_schema");
        public static final ColumnIdent TABLE_NAME = new ColumnIdent("table_name");
        public static final ColumnIdent CONSTRAINT_NAME = new ColumnIdent("constraint_name");
        public static final ColumnIdent CONSTRAINT_TYPE = new ColumnIdent("constraint_type");
    }

    public static class References {
        public static final Reference TABLE_SCHEMA = createRef(Columns.TABLE_SCHEMA, DataTypes.STRING);
        public static final Reference TABLE_NAME = createRef(Columns.TABLE_NAME, DataTypes.STRING);
        public static final Reference CONSTRAINT_NAME = createRef(Columns.CONSTRAINT_NAME, new ArrayType(DataTypes.STRING));
        public static final Reference CONSTRAINT_TYPE = createRef(Columns.CONSTRAINT_TYPE, DataTypes.STRING);
    }

    private static Reference createRef(ColumnIdent columnIdent, DataType dataType) {
        return new Reference(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationTableConstraintsTableInfo(ClusterService clusterService) {
        super(clusterService,
            IDENT,
            ImmutableList.<ColumnIdent>of(),
            ImmutableSortedMap.<ColumnIdent, Reference>naturalOrder()
                .put(Columns.TABLE_SCHEMA, References.TABLE_SCHEMA)
                .put(Columns.TABLE_NAME, References.TABLE_NAME)
                .put(Columns.CONSTRAINT_NAME, References.CONSTRAINT_NAME)
                .put(Columns.CONSTRAINT_TYPE, References.CONSTRAINT_TYPE)
                .build()
        );
    }
}
