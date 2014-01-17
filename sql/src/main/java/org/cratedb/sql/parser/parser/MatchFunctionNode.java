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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

public class MatchFunctionNode extends ValueNode {

    private ColumnReference columnReference;
    private ValueNode queryText;

    public void init(Object columnReference, Object queryText) {
        this.columnReference = (ColumnReference)columnReference;
        this.queryText = (ValueNode)queryText;
    }

    public ColumnReference getColumnReference() {
        return columnReference;
    }

    public ValueNode getQueryText() {
        return queryText;
    }

    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException {
        if (isSameNodeType(o)) {
            MatchFunctionNode other = (MatchFunctionNode)o;
            if (columnReference.equals(other.columnReference)
                    && queryText.equals(queryText)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        printLabel(depth, "columnReference:");
        columnReference.treePrint(depth+1);
        printLabel(depth, "queryText:");
        queryText.treePrint(depth+1);
    }

}
