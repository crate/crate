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
 * A SavepointNode is the root of a QueryTree that represents a Savepoint (ROLLBACK savepoint, RELASE savepoint and SAVEPOINT)
 * statement.
 */

public class SavepointNode extends DDLStatementNode
{
    public static enum StatementType {
        SET, ROLLBACK, RELEASE
    }
    private StatementType statementType;
    private String savepointName; // Name of the savepoint.

    /**
     * Initializer for a SavepointNode
     *
     * @param objectName The name of the savepoint
     * @param savepointStatementType Type of savepoint statement ie rollback, release or set savepoint
     *
     * @exception StandardException Thrown on error
     */

    public void init(Object objectName,
                     Object statementType)
            throws StandardException {
        initAndCheck(null);
        this.savepointName = (String)objectName;
        this.statementType = (StatementType)statementType;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SavepointNode other = (SavepointNode)node;
        this.statementType = other.statementType;
        this.savepointName = other.savepointName;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        String tempString = "savepointName: " + "\n" + savepointName + "\n";
        tempString = tempString + "savepointStatementType: " + "\n" + statementType + "\n";
        return super.toString() +    tempString;
    }

    public String statementToString() {
        switch (statementType) {
        case SET:
            return "SAVEPOINT";
        case ROLLBACK:
            return "ROLLBACK WORK TO SAVEPOINT";
        case RELEASE:
            return "RELEASE TO SAVEPOINT";
        default:
            assert false : "Unknown savepoint statement type";
            return "UNKNOWN";
        }
    }

}
