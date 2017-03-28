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
import org.elasticsearch.cluster.service.ClusterService;

public class InformationRoutinesTableInfo extends InformationTableInfo {

    public static final String NAME = "routines";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent ROUTINE_NAME = new ColumnIdent("routine_name");
        public static final ColumnIdent ROUTINE_TYPE = new ColumnIdent("routine_type");
    }

    public static class References {
        public static final Reference ROUTINE_NAME = info(Columns.ROUTINE_NAME, DataTypes.STRING);
        public static final Reference ROUTINE_TYPE = info(Columns.ROUTINE_TYPE, DataTypes.STRING);
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
                .build()
        );
    }
}
