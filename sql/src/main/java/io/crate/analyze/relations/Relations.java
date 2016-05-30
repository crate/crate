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

package io.crate.analyze.relations;

import io.crate.analyze.QueriedTable;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.metadata.table.Operation;

public class Relations {

    private static final SupportsOperationVisitor SUPPORTS_OPERATION_VISITOR = new SupportsOperationVisitor();

    public static boolean supportsOperation(AnalyzedRelation relation, Operation operation) {
        return SUPPORTS_OPERATION_VISITOR.process(relation, operation);
    }

    private static class SupportsOperationVisitor extends AnalyzedRelationVisitor<Operation, Boolean> {

        @Override
        protected Boolean visitAnalyzedRelation(AnalyzedRelation relation, Operation operation) {
            return false;
        }

        @Override
        public Boolean visitQueriedDocTable(QueriedDocTable table, Operation operation) {
            return process(table.tableRelation(), operation);
        }

        @Override
        public Boolean visitQueriedTable(QueriedTable table, Operation operation) {
            return table.tableRelation().tableInfo().supportedOperations().contains(operation);
        }

        @Override
        public Boolean visitDocTableRelation(DocTableRelation relation, Operation operation) {
            DocTableInfo tableInfo = relation.tableInfo();
            return tableInfo.supportedOperations().contains(operation) && (tableInfo.isPartitioned() || !tableInfo.isAlias());
        }
    }
}
