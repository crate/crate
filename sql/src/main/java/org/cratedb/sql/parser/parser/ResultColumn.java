/**
 * Copyright 2011-2013 Akiban Technologies, Inc.
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* The original from which this derives bore the following: */

/*

   Derby - Class org.apache.derby.impl.sql.compile.ResultColumn

   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to you under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.

 */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;
import org.cratedb.sql.parser.types.DataTypeDescriptor;

/**
 * A ResultColumn represents a result column in a SELECT, INSERT, or UPDATE
 * statement.    In a SELECT statement, the result column just represents an
 * expression in a row being returned to the client.    For INSERT and UPDATE
 * statements, the result column represents an column in a stored table.
 * So, a ResultColumn has to be bound differently depending on the type of
 * statement it appears in.
 * <P>
 * The type of the ResultColumn can differ from its underlying expression,
 * for example in certain joins the ResultColumn can be nullable even if
 * its underlying column is not. In an INSERT or UPDATE the ResultColumn
 * will represent the type of the column in the table, the type of
 * the underlying expresion will be the type of the source of the
 * value to be insert or updated. The method columnTypeAndLengthMatch()
 * can be used to detect when normalization is required between
 * the expression and the tyoe of ResultColumn. This class does
 * not implement any type normalization (conversion), this is
 * typically handled by a NormalizeResultSetNode.
 *
 */

public class ResultColumn extends ValueNode 
{
    /* name and exposedName should point to the same string, unless there is a
     * derived column list, in which case name will point to the underlying name
     * and exposedName will point to the name from the derived column list.
     */
    private String name;
    private String exposedName;
    private String tableName;
    private ValueNode expression;
    private boolean defaultColumn;

    // tells us if this ResultColumn represents an autoincrement column in a
    // base table.
    private boolean autoincrement;

    private ColumnReference reference; // used to verify quals at bind time, if given.

    /* virtualColumnId is the ResultColumn's position (1-based) within the ResultSet */
    private int virtualColumnId;

    private boolean isNameGenerated;

    /**
     * Different types of initializer parameters indicate different
     * types of initialization. Parameters may be:
     *
     * <ul>
     * <li>arg1 The name of the column, if any.</li>
     * <li>arg2 The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1 a column reference node</li>
     * <li>arg2 The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>arg1 The column descriptor.</li>
     * <li>arg2 The expression this result column represents</li>
     * </ul>
     *
     * <p>
     * - OR -
     * </p>
     *
     * <ul>
     * <li>dtd The type of the column</li>
     * <li>expression The expression this result column represents</li>
     * </ul>
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        // RESOLVE: This is something of a hack - it is not obvious that
        // the first argument being null means it should be treated as
        // a String.
        if ((arg1 instanceof String) || (arg1 == null)) {
            this.name = (String)arg1;
            this.exposedName = this.name;
            setExpression((ValueNode)arg2);
        }
        else if (arg1 instanceof ColumnReference) {
            ColumnReference ref = (ColumnReference)arg1;

            this.name = ref.getColumnName();
            this.exposedName = ref.getColumnName();
            /*
              when we bind, we'll want to make sure
              the reference has the right table name.
            */
            this.reference = ref; 
            setExpression((ValueNode)arg2);
        }
        else {
            setType((DataTypeDescriptor)arg1);
            setExpression((ValueNode)arg2);
            if (arg2 instanceof ColumnReference) {
                reference = (ColumnReference)arg2;
            }
        }

        /* this result column represents a <default> keyword in an insert or
         * update statement
         */
        if (expression != null &&
            expression.isInstanceOf(NodeType.DEFAULT_NODE))
            defaultColumn = true;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ResultColumn other = (ResultColumn)node;
        this.name = other.name;
        this.exposedName = other.exposedName;
        this.tableName = other.tableName;
        this.expression = (ValueNode)getNodeFactory().copyNode(other.expression,
                                                               getParserContext());
        this.defaultColumn = other.defaultColumn;
        this.autoincrement = other.autoincrement;
        this.reference = (ColumnReference)getNodeFactory().copyNode(other.reference,
                                                                    getParserContext());
        this.virtualColumnId = other.virtualColumnId;
        this.isNameGenerated = other.isNameGenerated;
    }

    /**
     * Returns TRUE if the ResultColumn is standing in for a DEFAULT keyword in
     * an insert/update statement.
     */
    public boolean isDefaultColumn() {
        return defaultColumn;
    }

    public void setDefaultColumn(boolean value) {
        defaultColumn = value;
    }

    /**
     * Return TRUE if this result column matches the provided column name.
     *
     * This function is used by ORDER BY column resolution. For the
     * ORDER BY clause, Derby will prefer to match on the column's
     * alias (exposedName), but will also successfully match on the
     * underlying column name. Thus the following statements are
     * treated equally:
     *  select name from person order by name;
     *  select name as person_name from person order by name;
     *  select name as person_name from person order by person_name;
     * See DERBY-2351 for more discussion.
     */
    boolean columnNameMatches(String columnName) {
        return columnName.equals(exposedName) ||
            columnName.equals(name) ||
            columnName.equals(getSourceColumnName());
    }
    /**
     * Returns the underlying source column name, if this ResultColumn
     * is a simple direct reference to a table column, or NULL otherwise.
     */
    String getSourceColumnName() {
        if (expression instanceof ColumnReference)
            return ((ColumnReference)expression).getColumnName();
        return null;
    }

    /**
     * The following methods implement the ResultColumnDescriptor
     * interface.    See the Language Module Interface for details.
     */
    public String getName() {
        return exposedName;
    }

    public String getSchemaName() throws StandardException {
        if (expression != null)
            return expression.getSchemaName();
        else
            return null;
    }
    
    public String getTableName() {
        if (tableName != null) {
            return tableName;
        }
        else if (expression != null)
            return expression.getTableName();
        else
            return null;
    }

    public int getColumnPosition() {
        return virtualColumnId;
    }

    /**
     * Set the expression in this ResultColumn.  This is useful in those
     * cases where you don't know the expression in advance, like for
     * INSERT statements with column lists, where the column list and
     * SELECT or VALUES clause are parsed separately, and then have to
     * be hooked up.
     *
     * @param expression The expression to be set in this ResultColumn
     */

    public void setExpression(ValueNode expression) {
        this.expression = expression;
    }

    /**
     * Get the expression in this ResultColumn.  
     *
     * @return ValueNode this.expression
     */

    public ValueNode getExpression() {
        return expression;
    }

    /**
     * Set the expression to a null node of the
     * correct type.
     *
     * @exception StandardException Thrown on error
     */
    void setExpressionToNullNode() throws StandardException {
        setExpression(getNullNode(getType()));
    }

    /**
     * Set the name in this ResultColumn.    This is useful when you don't
     * know the name at the time you create the ResultColumn, for example,
     * in an insert-select statement, where you want the names of the
     * result columns to match the table being inserted into, not the
     * table they came from.
     *
     * @param name The name to set in this ResultColumn
     */

    public void setName(String name) {
        if (this.name == null) {
            this.name = name;
        }
        else {
            assert (reference == null || name.equals(reference.getColumnName())) : 
                "don't change name from reference name";
        }

        this.exposedName = name;
    }

    /**
     * Is the name for this ResultColumn generated?
     */
    public boolean isNameGenerated() {
        return isNameGenerated;
    }

    /**
     * Set that this result column name is generated.
     */
    public void setNameGenerated(boolean value) {
        isNameGenerated = value;
    }

    /** 
     * Adjust the virtualColumnId for this ResultColumn by the specified amount
     * 
     * @param adjust The adjustment for the virtualColumnId
     */

    public void adjustVirtualColumnId(int adjust) {
        virtualColumnId += adjust;
    }

    /** 
     * Set the virtualColumnId for this ResultColumn
     * 
     * @param id The virtualColumnId for this ResultColumn
     */

    public void setVirtualColumnId(int id) {
        virtualColumnId = id;
    }

    /**
     * Get the virtualColumnId for this ResultColumn
     *
     * @return virtualColumnId for this ResultColumn
     */
    public int getVirtualColumnId() {
        return virtualColumnId;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "exposedName: " + exposedName + "\n" +
            "name: " + name + "\n" +
            "tableName: " + tableName + "\n" +
            "isDefaultColumn: " + defaultColumn + "\n" +
            super.toString();
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (expression != null) {
            printLabel(depth, "expression: ");
            expression.treePrint(depth + 1);
        }
        if (reference != null) {
            printLabel(depth, "reference: ");
            reference.treePrint(depth + 1);
        }
    }

    /**
     * Accept the visitor for all visitable children of this node.
     * 
     * @param v the visitor
     *
     * @exception StandardException on error
     */
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);

        if (expression != null) {
            setExpression((ValueNode)expression.accept(v));
        }
        if (reference != null) {
            reference = (ColumnReference)reference.accept(v);
        }
    }

    public TableName getTableNameObject() {
        return null;
    }

    /* Get the wrapped reference if any */
    public ColumnReference getReference() {
        return reference; 
    }

    public boolean isEquivalent(ValueNode o) throws StandardException {
        if (o.getNodeType() == getNodeType()) {                              
            ResultColumn other = (ResultColumn)o;
            if (expression != null) {
                return expression.isEquivalent(other.expression);
            }
        }
        return false;
    }
        
}
