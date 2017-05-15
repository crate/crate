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

import io.crate.exceptions.ResourceUnknownException;
import io.crate.metadata.Schemas;
import io.crate.metadata.TableIdent;
import io.crate.metadata.blob.BlobTableInfo;
import io.crate.operation.user.User;
import io.crate.sql.tree.DropBlobTable;

import javax.annotation.Nullable;

import static io.crate.analyze.BlobTableAnalyzer.tableToIdent;

class DropBlobTableAnalyzer {

    private final Schemas schemas;

    DropBlobTableAnalyzer(Schemas schemas) {
        this.schemas = schemas;
    }

    public DropBlobTableAnalyzedStatement analyze(DropBlobTable node, @Nullable User user) {
        TableIdent tableIdent = tableToIdent(node.table());
        BlobTableInfo tableInfo = null;
        boolean isNoop = false;
        try {
            tableInfo = schemas.getTableInfo(tableIdent, user);
        } catch (ResourceUnknownException e) {
            if (node.ignoreNonExistentTable()) {
                isNoop = true;
            } else {
                throw e;
            }
        }
        return new DropBlobTableAnalyzedStatement(tableInfo, isNoop, node.ignoreNonExistentTable());
    }
}
