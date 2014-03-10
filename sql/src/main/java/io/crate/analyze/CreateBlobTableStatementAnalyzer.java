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
import io.crate.sql.tree.*;

import java.util.List;
import java.util.Map;

public class CreateBlobTableStatementAnalyzer extends AbstractStatementAnalyzer<Void, CreateBlobTableAnalysis> {

    private final ExpressionToObjectVisitor expressionVisitor = new ExpressionToObjectVisitor();

    @Override
    public Void visitCreateBlobTable(CreateBlobTable node, CreateBlobTableAnalysis context) {
        List<String> tableNameParts = node.name().getName().getParts();
        Preconditions.checkArgument(tableNameParts.size() == 1,
                "blob table cannot be created inside a custom schema");
        context.table(new TableIdent("blob", tableNameParts.get(0)));

        if (node.clusteredBy().isPresent()) {
            ClusteredBy clusteredBy = node.clusteredBy().get();
            context.numberOfShards(clusteredBy.numberOfShards().orNull());
        }

        if (node.genericProperties().isPresent()) {
            Map<String,List<Expression>> properties = node.genericProperties().get().properties();
            List<Expression> number_of_replicas = properties.remove("number_of_replicas");
            if (number_of_replicas != null) {
                setNumberOfReplicas(context, number_of_replicas);
            }

            if (properties.size() > 0) {
                throw new IllegalArgumentException(
                        String.format("Invalid properties \"%s\" passed to CREATE BLOB TABLE",
                                properties.keySet()));
            }
        }

        return null;
    }

    private void setNumberOfReplicas(CreateBlobTableAnalysis context, List<Expression> number_of_replicas) {
        Preconditions.checkArgument(number_of_replicas.size() == 1,
                "Invalid number of arguments passed to \"number_of_replicas\"");

        Object numReplicas = expressionVisitor.process(number_of_replicas.get(0), context.parameters());
        if (numReplicas != null) {
            context.numberOfReplicas(numReplicas.toString());
        }
    }
}
