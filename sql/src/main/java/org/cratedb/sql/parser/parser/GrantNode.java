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

import java.util.List;
import java.util.Iterator;

/**
 * This class represents a GRANT statement.
 */
public class GrantNode extends DDLStatementNode
{
    private PrivilegeNode privileges;
    private List<String> grantees;

    /**
     * Initialize a GrantNode.
     *
     * @param privileges PrivilegesNode
     * @param grantees List
     */
    public void init(Object privileges, Object grantees) {
        this.privileges = (PrivilegeNode)privileges;
        this.grantees = (List<String>)grantees;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        GrantNode other = (GrantNode)node;
        this.privileges = (PrivilegeNode)getNodeFactory().copyNode(other.privileges,
                                                                   getParserContext());
        this.grantees = other.grantees; // TODO: Clone?
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        StringBuffer sb = new StringBuffer();
        for (Iterator<String> it = grantees.iterator(); it.hasNext();) {
            if (sb.length() > 0)
                sb.append( ",");
            sb.append(it.next());
        }
        return super.toString() +
            privileges.toString() +
            "TO: \n" + sb.toString() + "\n";
    }

    public String statementToString() {
        return "GRANT";
    }

}
