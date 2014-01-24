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
import org.cratedb.sql.parser.types.DataTypeDescriptor;

/**
 * A ColumnDefinitionNode represents a column definition in a DDL statement.
 * There will be a ColumnDefinitionNode for each column in a CREATE TABLE
 * statement, and for the column in an ALTER TABLE ADD COLUMN statement.
 *
 */

public class ColumnDefinitionNode extends TableElementNode
{
    boolean isAutoincrement;

    /**
     * The data type of this column.
     */
    DataTypeDescriptor type;
        
    DefaultNode defaultNode;
    boolean keepCurrentDefault;
    GenerationClauseNode generationClauseNode;
    long autoincrementIncrement;
    long autoincrementStart;
    //This variable tells if the autoincrement column is participating 
    //in create or alter table. And if it is participating in alter
    //table, then it further knows if it is represting a change in 
    //increment value or a change in start value.
    //This information is later used to make sure that the autoincrement
    //column's increment value is not 0 at the time of create, or is not
    //getting set to 0 at the time of increment value modification.
    long autoinc_create_or_modify_Start_Increment;
    boolean autoincrementVerify;

    //autoinc_create_or_modify_Start_Increment will be set to one of the
    //following 3 values.
    //CREATE_AUTOINCREMENT - this autoincrement column definition is for create table
    public static final int CREATE_AUTOINCREMENT = 0;
    //MODIFY_AUTOINCREMENT_RESTART_VALUE - this column definition is for
    //alter table command to change the start value of the column
    public static final int MODIFY_AUTOINCREMENT_RESTART_VALUE = 1;
    //MODIFY_AUTOINCREMENT_INC_VALUE - this column definition is for
    //alter table command to change the increment value of the column
    public static final int MODIFY_AUTOINCREMENT_INC_VALUE = 2;

    /**
     * Initializer for a ColumnDefinitionNode
     *
     * @param name The name of the column
     * @param defaultNode The default value of the column
     * @param type A DataTypeDescriptor telling the type of the column
     * @param autoIncrementInfo Info for autoincrement columns
     *
     */

    public void init(Object name,
                     Object defaultNode,
                     Object type,
                     Object autoIncrementInfo) throws StandardException {
        super.init(name);
        this.type = (DataTypeDescriptor)type;
        if (defaultNode instanceof UntypedNullConstantNode) {
            // TODO: Can make properly typed null using this.type now.
        }
        else if (defaultNode instanceof GenerationClauseNode) {
            generationClauseNode = (GenerationClauseNode)defaultNode;
        }
        else {
            assert (defaultNode == null || (defaultNode instanceof DefaultNode));
            this.defaultNode = (DefaultNode)defaultNode;
            if (autoIncrementInfo != null) {
                long[] aii = (long[])autoIncrementInfo;
                autoincrementStart = aii[QueryTreeNode.AUTOINCREMENT_START_INDEX];
                autoincrementIncrement = aii[QueryTreeNode.AUTOINCREMENT_INC_INDEX];
                //Parser has passed the info about autoincrement column's status in the
                //following array element. It will tell if the autoinc column is part of 
                //a create table or if is a part of alter table. And if it is part of 
                //alter table, is it for changing the increment value or for changing 
                //the start value?
                autoinc_create_or_modify_Start_Increment = aii[QueryTreeNode.AUTOINCREMENT_CREATE_MODIFY];

                /*
                 * If using DB2 syntax to set increment value, will need to check if column
                 * is already created for autoincrement.
                 */
                autoincrementVerify = (aii[QueryTreeNode.AUTOINCREMENT_IS_AUTOINCREMENT_INDEX] > 0) ? false : true;
                isAutoincrement = true;
                // an autoincrement column cannot be null-- setting
                // non-nullability for this column is needed because 
                // you could create a column with ai default, add data, drop 
                // the default, and try to add it back again you'll get an
                // error because the column is marked nullable.
                if (type != null)
                    setNullability(false);
            }
        }
        // ColumnDefinitionNode instances can be subclassed by
        // ModifyColumnNode for use in ALTER TABLE .. ALTER COLUMN
        // statements, in which case the node represents the intended
        // changes to the column definition. For such a case, we
        // record whether or not the statement specified that the
        // column's default value should be changed. If we are to
        // keep the current default, ModifyColumnNode will re-read
        // the current default from the system catalogs prior to
        // performing the column alteration. See DERBY-4006
        // for more discussion of this behavior.
        this.keepCurrentDefault = (defaultNode == null);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ColumnDefinitionNode other = (ColumnDefinitionNode)node;
        this.isAutoincrement = other.isAutoincrement;
        this.type = other.type;
        this.defaultNode = (DefaultNode)
            getNodeFactory().copyNode(other.defaultNode, getParserContext());
        this.keepCurrentDefault = other.keepCurrentDefault;
        this.generationClauseNode = (GenerationClauseNode)
            getNodeFactory().copyNode(other.generationClauseNode, getParserContext());
        this.autoincrementIncrement = other.autoincrementIncrement;
        this.autoincrementStart = other.autoincrementStart;
        this.autoinc_create_or_modify_Start_Increment = other.autoinc_create_or_modify_Start_Increment;
        this.autoincrementVerify = other.autoincrementVerify;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "type: " + getType() + "\n" +
            (!isAutoincrementColumn() ? "" : (
             "autoIncrementStart: " + getAutoincrementStart() + "\n" +
             "autoIncrementIncrement: " + getAutoincrementIncrement() + "\n"
            )) +
            super.toString();
    }

    /**
     * Returns the unqualified name of the column being defined.
     *
     * @return the name of the column
     */
    public String getColumnName() {
        return this.name;
    }

    /**
     * Returns the data type of the column being defined.
     *
     * @return the data type of the column
     */
    public final DataTypeDescriptor getType() {
        return type;
    }

    /** Set the type of this column */
    public void setType(DataTypeDescriptor dts) { 
        type = dts; 
    }
        
    /**
     * Set the nullability of the column definition node.
     */
    void setNullability(boolean nullable) {
        type = getType().getNullabilityType(nullable);
    }

    /**
     * Return the DefaultNode, if any, associated with this node.
     *
     * @return The DefaultNode, if any, associated with this node.
     */
    public DefaultNode getDefaultNode() {
        return defaultNode;
    }

    /**
     * Return true if this column has a generation clause.
     */
    public boolean hasGenerationClause() { 
        return (generationClauseNode != null); 
    }

    /**
     * Get the generation clause.
     */
    public GenerationClauseNode getGenerationClauseNode() { 
        return generationClauseNode; 
    }
    
    /**
     * Is this an autoincrement column?
     *
     * @return Whether or not this is an autoincrement column.
     */
    public boolean isAutoincrementColumn() {
        return isAutoincrement;
    }

    /**
     * Get the autoincrement start value
     *
     * @return Autoincrement start value.
     */
    public long getAutoincrementStart()
    {
        return autoincrementStart;
    }

    /**
     * Get the autoincrement increment value
     *
     * @return Autoincrement increment value.
     */
    public long getAutoincrementIncrement()
    {
        return autoincrementIncrement;
    }

    /**
     * Get the status of this autoincrement column 
     *
     * @return ColumnDefinitionNode.CREATE_AUTOINCREMENT - 
     *               if this definition is for autoincrement column creatoin
     *   ColumnDefinitionNode.MODIFY_AUTOINCREMENT_RESTART_VALUE -
     *               if this definition is for alter sutoincrement column to change the start value 
     *   ColumnDefinitionNode.MODIFY_AUTOINCREMENT_INC_VALUE 
     *               if this definition is for alter autoincrement column to change the increment value
     */
    public long getAutoinc_create_or_modify_Start_Increment()
    {
        return autoinc_create_or_modify_Start_Increment;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (defaultNode != null) {
            printLabel(depth, "default: ");
            defaultNode.treePrint(depth + 1);
        }


        if (generationClauseNode != null) {
            printLabel(depth, "generationClause: ");
            generationClauseNode.treePrint(depth + 1);
        }
    }
}
