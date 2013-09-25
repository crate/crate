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

   Derby - Class org.apache.derby.impl.sql.compile.ResultSetNode

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

/**
 * A ResultSetNode represents a result set, that is, a set of rows.  It is
 * analogous to a ResultSet in the LanguageModuleExternalInterface.  In fact,
 * code generation for a a ResultSetNode will create a "new" call to a
 * constructor for a ResultSet.
 *
 */

public abstract class ResultSetNode extends QueryTreeNode
{
    ResultColumnList resultColumns;
    boolean insertSource;

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ResultSetNode other = (ResultSetNode)node;
        this.resultColumns = (ResultColumnList)getNodeFactory().copyNode(other.resultColumns,
                                                                         getParserContext());
        this.insertSource = other.insertSource;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return super.toString();
    }

    /**
     * Remember that this node is the source result set for an INSERT.
     */
    public void setInsertSource() {
        insertSource = true;
    }

    /**
     * Set the resultColumns in this ResultSetNode
     *
     * @param newRCL The new ResultColumnList for this ResultSetNode
     */
    public void setResultColumns(ResultColumnList newRCL) {
        resultColumns = newRCL;
    }

    /**
     * Get the resultColumns for this ResultSetNode
     *
     * @return ResultColumnList for this ResultSetNode
     */
    public ResultColumnList getResultColumns() {
        return resultColumns;
    }

    /**
     * Prints the sub-nodes of this object.  See QueryTreeNode.java for
     * how tree printing is supposed to work.
     *
     * @param depth The depth of this node in the tree
     */

    public void printSubNodes(int depth) {
        super.printSubNodes(depth);

        if (resultColumns != null) {
            printLabel(depth, "resultColumns: ");
            resultColumns.treePrint(depth + 1);
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

        if (resultColumns != null) {
            resultColumns = (ResultColumnList)resultColumns.accept(v);
        }
    }

}
