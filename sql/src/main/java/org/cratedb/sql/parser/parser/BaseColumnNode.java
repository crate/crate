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

   Derby - Class org.apache.derby.impl.sql.compile.BaseColumnNode

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
 * A BaseColumnNode represents a column in a base table.    The parser generates a
 * BaseColumnNode for each column reference.    A column refercence could be a column in
 * a base table, a column in a view (which could expand into a complex
 * expression), or a column in a subquery in the FROM clause.    By the time
 * we get to code generation, all BaseColumnNodes should stand only for columns
 * in base tables.
 *
 */

public class BaseColumnNode extends ValueNode
{
    private String columnName;

    /*
    ** This is the user-specified table name.    It will be null if the
    ** user specifies a column without a table name.    
    */
    private TableName tableName;

    /**
     * Initializer for when you only have the column name.
     *
     * @param columnName The name of the column being referenced
     * @param tableName The qualification for the column
     * @param type DataTypeDescriptor for the column
     */

    public void init(Object columnName,
                     Object tableName,
                     Object type) 
            throws StandardException {
        this.columnName = (String)columnName;
        this.tableName = (TableName)tableName;
        setType((DataTypeDescriptor)type);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        BaseColumnNode other = (BaseColumnNode)node;
        this.columnName = other.columnName;
        this.tableName = (TableName)getNodeFactory().copyNode(other.tableName, 
                                                              getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "columnName: " + columnName + "\n" +
            "tableName: " +
            ( ( tableName != null) ?
              tableName.toString() :
              "null") + "\n" +
            super.toString();
    }

    /**
     * Get the name of this column
     *
     * @return The name of this column
     */

    public String getColumnName()
    {
        return columnName;
    }

    /**
     * Get the user-supplied table name of this column.  This will be null
     * if the user did not supply a name (for example, select a from t).
     * The method will return B for this example, select b.a from t as b
     * The method will return T for this example, select t.a from t
     *
     * @return The user-supplied name of this column.    Null if no user-
     *               supplied name.
     */

    public String getTableName() {
        return ((tableName != null) ? tableName.getTableName() : null);
    }

    /**
     * Get the user-supplied schema name for this column's table. This will be null
     * if the user did not supply a name (for example, select t.a from t).
     * Another example for null return value (for example, select b.a from t as b).
     * But for following query select app.t.a from t, this will return APP
     *
     * @return The schema name for this column's table
     */
    public String getSchemaName() throws StandardException {
        return ((tableName != null) ? tableName.getSchemaName() : null);
    }
                
    /**
     * {@inheritDoc}
     */
    protected boolean isEquivalent(ValueNode o) {
        if (isSameNodeType(o)) {
            BaseColumnNode other = (BaseColumnNode)o;
            return other.tableName.equals(tableName) &&
                other.columnName.equals(columnName);
        } 
        return false;
    }
}
