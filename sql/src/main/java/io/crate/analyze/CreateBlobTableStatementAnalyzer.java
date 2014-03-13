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

import io.crate.core.NumberOfReplicas;
import io.crate.sql.tree.ClusteredBy;
import io.crate.sql.tree.CreateBlobTable;

public class CreateBlobTableStatementAnalyzer extends BlobTableAnalyzer<CreateBlobTableAnalysis> {


    @Override
    public Void visitCreateBlobTable(CreateBlobTable node, CreateBlobTableAnalysis context) {
        context.table(tableToIdent(node.name()));

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();
            context.numberOfShards(clusteredBy.numberOfShards().orNull());
        }

        if (node.genericProperties().isPresent()) {
            NumberOfReplicas numberOfReplicas =
                    extractNumberOfReplicas(node.genericProperties().get(), context.parameters());
            context.numberOfReplicas(numberOfReplicas);
        }

        return null;
    }
}
