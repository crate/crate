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


import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.CrateException;
import org.cratedb.sql.SQLParseException;
import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.parser.*;

/**
 * Visitor that dispatches generic calls to {@link #visit(org.cratedb.sql.parser.parser.Visitable)}
 * and {@link #visit(org.cratedb.sql.parser.parser.ValueNode, org.cratedb.sql.parser.parser.ValueNode)}
 * into more specific method calls.
 *
 * E.g. visit(ValueNode, ValueNode) might become visit(ValueNode, AndNode) depending on the exact nodeType
 */
public abstract class DispatchingVisitor implements Visitor {

    protected final ParsedStatement stmt;
    protected boolean stopTraversal = false;

    protected void visit(ValueNode parentNode, BinaryRelationalOperatorNode node) throws  Exception {}
    protected void visit(ValueNode parentNode, AndNode node) throws Exception {}
    protected void visit(ValueNode parentNode, OrNode node) throws Exception {}
    protected void visit(ValueNode parentNode, IsNullNode node) throws Exception {}
    protected void visit(ValueNode parentNode, LikeEscapeOperatorNode node) throws Exception {}
    protected void visit(ValueNode parentNode, InListOperatorNode node) throws Exception {}
    protected void visit(ValueNode parentNode, NotNode node) throws Exception {}
    protected void visit(ValueNode parentNode, MatchFunctionNode node) throws Exception {}
    protected void visit(CursorNode node) throws Exception {}
    protected void visit(UpdateNode node) throws Exception {}
    protected void visit(DeleteNode node) throws Exception {}
    protected void visit(InsertNode node) throws Exception {}
    protected void visit(CreateTableNode node) throws Exception {}
    protected void visit(DropTableNode node) throws Exception {}
    protected void visit(ColumnDefinitionNode node) throws Exception {}
    protected void visit(ObjectColumnDefinitionNode node) throws Exception {}
    protected void visit(ConstraintDefinitionNode node) throws Exception {}
    protected void visit(CreateAnalyzerNode node) throws Exception {}
    protected void visit(IndexConstraintDefinitionNode node) throws Exception {}
    protected void visit(CopyStatementNode node) throws Exception {}

    protected void afterVisit() throws SQLParseException {}

    public DispatchingVisitor(ParsedStatement parsedStatement) {
        this.stmt = parsedStatement;
    }

    @Override
    public Visitable visit(Visitable node) throws StandardException {
        try {
            switch (((QueryTreeNode)node).getNodeType()) {
                case CURSOR_NODE:
                    stopTraversal = true;
                    visit((CursorNode) node);
                    break;
                case UPDATE_NODE:
                    stopTraversal = true;
                    visit((UpdateNode)node);
                    break;
                case DELETE_NODE:
                    stopTraversal = true;
                    visit((DeleteNode)node);
                    break;
                case INSERT_NODE:
                    stopTraversal = true;
                    visit((InsertNode)node);
                    break;
                case CREATE_TABLE_NODE:
                    stopTraversal = true;
                    visit((CreateTableNode)node);
                    break;
                case DROP_TABLE_NODE:
                    stopTraversal = true;
                    visit((DropTableNode)node);
                    break;
                case CREATE_ANALYZER_NODE:
                    stopTraversal = true;
                    visit((CreateAnalyzerNode)node);
                    break;
                case COPY_STATEMENT_NODE:
                    stopTraversal = true;
                    visit((CopyStatementNode)node);
                    break;
                default:
                    throw new SQLParseException("Unsupported SQL Statement");
            }
        } catch (CrateException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new SQLParseException(ex.getMessage(), ex);
        }

        if (stopTraversal) {
            stmt.nodeType(((QueryTreeNode) node).getNodeType());
            afterVisit();
        }
        return node;
    }

    protected void visit(TableElementNode tableElement) throws Exception {
        switch(tableElement.getNodeType()) {
            case COLUMN_DEFINITION_NODE:
                visit((ColumnDefinitionNode) tableElement);
                break;
            case OBJECT_COLUMN_DEFINITION_NODE:
                visit((ObjectColumnDefinitionNode)tableElement);
                break;
            case CONSTRAINT_DEFINITION_NODE:
                visit((ConstraintDefinitionNode)tableElement);
                break;
            case INDEX_CONSTRAINT_NODE:
                visit((IndexConstraintDefinitionNode)tableElement);
                break;
        }
    }

    protected void visit(ValueNode parentNode, ValueNode node) throws Exception {
        switch (node.getNodeType()) {
            case IN_LIST_OPERATOR_NODE:
                visit(parentNode, (InListOperatorNode)node);
                return;
            case LIKE_OPERATOR_NODE:
                visit(parentNode, (LikeEscapeOperatorNode)node);
                return;
            case OR_NODE:
                visit(parentNode, (OrNode)node);
                return;
            case NOT_NODE:
                visit(parentNode, (NotNode)node);
                return;
            case AND_NODE:
                visit(parentNode, (AndNode)node);
                return;
            case IS_NULL_NODE:
                visit(parentNode, (IsNullNode)node);
                return;
            case MATCH_FUNCTION_NODE:
                visit(parentNode, (MatchFunctionNode)node);
                return;
        }

        if (node instanceof BinaryRelationalOperatorNode) {
            visit(parentNode, (BinaryRelationalOperatorNode) node);
        } else {
            throw new SQLParseException("Unhandled node: " + node.toString());
        }
    }

    @Override
    public boolean visitChildrenFirst(Visitable node) {
        return false;
    }

    @Override
    public boolean stopTraversal() {
        return stopTraversal;
    }

    @Override
    public boolean skipChildren(Visitable node) throws StandardException {
        return false;
    }
}
