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

public class CreateAnalyzerNode extends DDLStatementNode {

    private AnalyzerElements analyzerElements = null;

    private TableName extendsName = null;

    @Override
    public void init(
            Object name,
            Object extendsName,
            Object analyzerElements) throws StandardException {
        super.init(name);
        this.extendsName = (TableName) extendsName;
        this.analyzerElements = (AnalyzerElements) analyzerElements;
    }

    @Override
    public String statementToString() {
        return "CREATE ANALYZER";
    }

    public TableName getExtendsName() {
        return extendsName;
    }

    public AnalyzerElements getElements() {
        return analyzerElements;
    }

    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        CreateAnalyzerNode createAnalyzerNode = (CreateAnalyzerNode) node;
        this.extendsName = (TableName)getNodeFactory().copyNode(createAnalyzerNode.getExtendsName(), getParserContext());
        this.analyzerElements = (AnalyzerElements)getNodeFactory().copyNode(createAnalyzerNode.getElements(), getParserContext());
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (extendsName != null) {
            printLabel(depth, "extends: ");
            extendsName.treePrint(depth+1);
        }
        if (analyzerElements != null) {
            analyzerElements.treePrint(depth+1);
        }
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        if (extendsName != null) {
            extendsName.accept(v);
        }
        analyzerElements.accept(v);
    }
}
