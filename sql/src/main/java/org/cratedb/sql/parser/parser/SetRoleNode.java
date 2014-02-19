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
 * A SetRoleNode is the root of a QueryTree that represents a SET ROLE
 * statement.
 */

public class SetRoleNode extends MiscellaneousStatementNode
{
    private String name;
    private int type;

    /**
     * Initializer for a SetRoleNode
     *
     * @param roleName The name of the new role, null if NONE specified
     * @param type Type of role name could be USER or dynamic parameter
     *
     */
    public void init(Object roleName, Object type) {
        this.name = (String)roleName;
        if (type != null) {
            this.type = ((Integer)type).intValue();
        }
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        SetRoleNode other = (SetRoleNode)node;
        this.name = other.name;
        this.type = other.type;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return  This object as a String
     */

    public String toString() {
        return super.toString() +
            (type == StatementType.SET_ROLE_DYNAMIC ?
             "roleName: ?\n" :
             "rolename: " + name + "\n");
    }

    public String statementToString() {
        return "SET ROLE";
    }

}
