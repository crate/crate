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

import io.crate.sql.tree.AstVisitor;
import io.crate.sql.tree.CreateTable;
import io.crate.sql.tree.Node;
import io.crate.sql.tree.TableElement;

public class CreateTableStatementAnalyzer extends AstVisitor {

    @Override
    protected Object visitNode(Node node, Object context) {
        throw new RuntimeException(
                String.format("Encountered node %s but expected a CreateTable node", node));
    }

    @Override
    public Object visitCreateTable(CreateTable node, Object context) {
        node.clusteredBy();
        node.name();
        node.replicas();
        for (TableElement tableElement : node.tableElements()) {

        }

        return null;
    }
}
