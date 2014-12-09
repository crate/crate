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

public class InformationSchemataTableInfo extends InformationTableInfo {

    public static final String NAME = "schemata";
    public static final TableIdent IDENT = new TableIdent(InformationSchemaInfo.NAME, NAME);

    public static class Columns {
        public static final ColumnIdent SCHEMA_NAME = new ColumnIdent("schema_name");
    }

    public static class ReferenceInfos {
        public static final ReferenceInfo SCHEMA_NAME = info(Columns.SCHEMA_NAME, DataTypes.STRING);
    }

    private static ReferenceInfo info(ColumnIdent columnIdent, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(IDENT, columnIdent), RowGranularity.DOC, dataType);
    }

    protected InformationSchemataTableInfo(InformationSchemaInfo schemaInfo) {
        super(schemaInfo, IDENT,
                ImmutableList.of(Columns.SCHEMA_NAME),
                ImmutableMap.of(Columns.SCHEMA_NAME, ReferenceInfos.SCHEMA_NAME),
                ImmutableList.of(ReferenceInfos.SCHEMA_NAME)
        );
    }
}
