/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.analyze;

import com.google.common.base.Preconditions;
import io.crate.metadata.RelationName;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.tree.Table;

import java.util.List;

final class BlobTableAnalyzer {

    private BlobTableAnalyzer() {
    }

    static RelationName tableToIdent(Table table) {
        List<String> tableNameParts = table.getName().getParts();
        Preconditions.checkArgument(tableNameParts.size() < 3, "Invalid tableName \"%s\"", table.getName());

        if (tableNameParts.size() == 2) {
            Preconditions.checkArgument(tableNameParts.get(0).equalsIgnoreCase(BlobSchemaInfo.NAME),
                "The Schema \"%s\" isn't valid in a [CREATE | ALTER] BLOB TABLE clause",
                tableNameParts.get(0));

            return new RelationName(tableNameParts.get(0), tableNameParts.get(1));
        }
        assert tableNameParts.size() == 1 : "tableNameParts.size() must be 1";
        return new RelationName(BlobSchemaInfo.NAME, tableNameParts.get(0));
    }
}
