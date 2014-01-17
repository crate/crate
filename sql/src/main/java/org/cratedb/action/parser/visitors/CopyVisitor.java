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

package org.cratedb.action.parser.visitors;

import org.cratedb.action.sql.NodeExecutionContext;
import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.parser.CopyStatementNode;

public class CopyVisitor extends BaseVisitor {

    public CopyVisitor(NodeExecutionContext context, ParsedStatement parsedStatement, Object[] args) {
        super(context, parsedStatement, args);
    }

    @Override
    public void visit(CopyStatementNode node) throws Exception {
        tableName(node.getTableName());

        stmt.importPath = (String)valueFromNode(node.getFilename());

        stmt.type(ParsedStatement.ActionType.COPY_IMPORT_ACTION);

        if (tableContext.tableIsAlias()) {
            throw new SQLParseException("Table alias not allowed in COPY statement.");
        }
    }
}
