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
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobSchemaInfo;
import io.crate.sql.tree.DropBlobTable;

import java.util.List;

public class DropBlobTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, DropBlobTableAnalysis> {

    @Override
    public Void visitDropBlobTable(DropBlobTable node, DropBlobTableAnalysis context) {
        List<String> tableNameParts = node.table().getName().getParts();
        Preconditions.checkArgument(tableNameParts.size() < 3, "Invalid tableName \"%s\"", node.table().getName());

        if (tableNameParts.size() == 2) {
            Preconditions.checkArgument(tableNameParts.get(0).equalsIgnoreCase(BlobSchemaInfo.NAME),
                    "The Schema \"%s\" isn't valid. Can't delete a blob table from a SCHEMA other than \"%s\".",
                    tableNameParts.get(0), BlobSchemaInfo.NAME);

            context.table(new TableIdent(tableNameParts.get(0), tableNameParts.get(1)));
        } else {
            assert tableNameParts.size() == 1;
            context.table(new TableIdent(BlobSchemaInfo.NAME, tableNameParts.get(0)));
        }

        return null;
    }
}
