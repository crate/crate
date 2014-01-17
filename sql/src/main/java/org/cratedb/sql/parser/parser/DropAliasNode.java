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
import org.cratedb.sql.parser.types.AliasInfo;

/**
 * A DropAliasNode  represents a DROP ALIAS statement.
 *
 */

public class DropAliasNode extends DDLStatementNode
{
    private AliasInfo.Type aliasType;
    private ExistenceCheck existenceCheck;

    /**
     * Initializer for a DropAliasNode
     *
     * @param dropAliasName The name of the method alias being dropped
     * @param aliasType Alias type
     *
     * @exception StandardException
     */
    public void init(Object dropAliasName, Object aliasType, Object existenceCheck) throws StandardException {
        TableName dropItem = (TableName)dropAliasName;
        initAndCheck(dropItem);
        this.aliasType = (AliasInfo.Type)aliasType;
        this.existenceCheck = (ExistenceCheck)existenceCheck;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        DropAliasNode other = (DropAliasNode)node;
        this.aliasType = other.aliasType;
        this.existenceCheck = other.existenceCheck;
    }

    public AliasInfo.Type getAliasType() { 
        return aliasType; 
    }

    public ExistenceCheck getExistenceCheck()
    {
        return existenceCheck;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    public String toString() {
        return super.toString() +
            "existenceCheck: " + existenceCheck + "\n";
    }

    public String statementToString() {
        return "DROP " + aliasTypeName(aliasType);
    }

    /* returns the alias type name given the alias char type */
    private static String aliasTypeName(AliasInfo.Type type) {
        String typeName = null;
        switch (type) {
        case PROCEDURE:
            typeName = "PROCEDURE";
            break;
        case FUNCTION:
            typeName = "FUNCTION";
            break;
        case SYNONYM:
            typeName = "SYNONYM";
            break;
        case UDT:
            typeName = "TYPE";
            break;
        }
        return typeName;
    }

}
