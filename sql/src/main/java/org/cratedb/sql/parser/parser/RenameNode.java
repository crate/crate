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

/* The original from which this derives bore the following: */

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

/**
 * A RenameNode is the root of a QueryTree that represents a
 * RENAME TABLE/COLUMN/INDEX statement.
 *
 */

public class RenameNode extends DDLStatementNode
{
    protected TableName newTableName;

    // original name of the object being renamed
    protected String oldObjectName;
    // original name for that object
    protected String newObjectName;

    /* You can rename using either alter table or rename command to
     * rename a table/column. An index can only be renamed with rename
     * command. usedAlterTable flag is used to keep that information.
     */
    protected boolean usedAlterTable;

    public static enum RenameType {
        TABLE, COLUMN, INDEX
    }
    protected RenameType renamingWhat;

    /**
     * Initializer for a RenameNode
     *
     * @param tableName The name of the table. This is the table which is
     *              being renamed in case of rename table. In case of rename
     *              column, the column being renamed belongs to this table.
     *              In case of rename index, this is null because index name
     *              is unique within a schema and doesn't have to be
     *              associated with a table name
     * @param oldObjectName This is either the name of column/index in case
     *              of rename column/index. For rename table, this is null.
     * @param newObjectName This is new name for table/column/index
     * @param usedAlterTable True-Used Alter Table, False-Used Rename.
     *              For rename index, this will always be false because
     *              there is no alter table command to rename index
     * @param renamingWhat Rename a table / column / index
     *
     * @exception StandardException Thrown on error
     */
    public void init(Object tableName,
                     Object oldObjectName,
                     Object newName,
                     Object usedAlterTable,
                     Object renamingWhat)
            throws StandardException {
        this.usedAlterTable = ((Boolean)usedAlterTable).booleanValue();
        this.renamingWhat = (RenameType)renamingWhat;

        switch (this.renamingWhat) {
        case TABLE:
            initAndCheck((TableName)tableName);
            this.newTableName = (TableName)newName;
            this.oldObjectName = null;
            this.newObjectName = this.newTableName.getTableName();
            break;

        case INDEX:
            initAndCheck((TableName)tableName);
            this.oldObjectName = (String)oldObjectName;
            this.newObjectName = (String)newName;
            break;
            
        case COLUMN:
            /* coming from ALTER TABLE path, tableName will
             * be TableName object. Coming from RENAME COLUMN
             * path, tableName will be just a String.
             */
            TableName actingObjectName;
            if (tableName instanceof TableName)
                actingObjectName = (TableName)tableName;
            else
                actingObjectName = makeTableName(null, (String)tableName);
            initAndCheck(actingObjectName);

            this.oldObjectName = (String)oldObjectName;
            this.newObjectName = (String)newName;
            break;

        default:
            assert false : "Unexpected rename action in RenameNode";
        }
    }

    public RenameType getRenameType() {
        return renamingWhat;
    }

    public TableName getNewTableName() {
        return newTableName;
    }

    public boolean isAlterTable() {
        return usedAlterTable;
    }

    public String getOldObjectName() {
        return oldObjectName;
    }

    public String getNewObjectName() {
        return newObjectName;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        RenameNode other = (RenameNode)node;
        this.newTableName = (TableName)getNodeFactory().copyNode(other.newTableName,
                                                                 getParserContext());
        this.oldObjectName = other.oldObjectName;
        this.newObjectName = other.newObjectName;
        this.usedAlterTable = other.usedAlterTable;
        this.renamingWhat = other.renamingWhat;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        switch (renamingWhat) {
        case TABLE:
            return super.toString() +
                "oldTableName: " + "\n" + getRelativeName() + "\n" +
                "newTableName: " + "\n" + newTableName + "\n" ;

        case COLUMN:
            return super.toString() +
                "oldTableName.oldColumnName:" + "\n" +
                getRelativeName() + "." + oldObjectName + "\n" +
                "newColumnName: " + "\n" + newObjectName + "\n" ;

        case INDEX:
            return super.toString() +
                "oldIndexName:" + "\n" + oldObjectName + "\n" +
                "newIndexName: " + "\n" + newObjectName + "\n" ;

        default:
            assert false : "Unexpected rename action in RenameNode";
            return "UNKNOWN";
        }
    }

    public String statementToString() {
        if (usedAlterTable) {
            switch (renamingWhat) {
            case TABLE:
                return "ALTER TABLE";

            default:
                assert false : "Unexpected rename action in RenameNode";
                return "UNKNOWN";
            }
        }
        else {
            switch (renamingWhat) {
            case TABLE:
                return "RENAME TABLE";

            case COLUMN:
                return "RENAME COLUMN";

            case INDEX:
                return "RENAME INDEX";

            default:
                assert false : "Unexpected rename action in RenameNode";
                return "UNKNOWN";
            }
        }
    }

}
