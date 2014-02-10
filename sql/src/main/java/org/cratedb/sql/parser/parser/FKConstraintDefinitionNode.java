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
 * A FKConstraintDefintionNode represents table constraint definitions.
 *
 */

public class FKConstraintDefinitionNode extends ConstraintDefinitionNode
{
    TableName refTableName;
    ResultColumnList refRcl;
    int refActionDeleteRule;    // referential action on delete
    int refActionUpdateRule;    // referential action on update
    boolean grouping;

    // For ADD
    public void init(Object constraintName, 
                     Object refTableName, 
                     Object fkRcl,
                     Object refRcl,
                     Object refActions,
                     Object grouping) {
        super.init(constraintName,
                   ConstraintType.FOREIGN_KEY,
                   fkRcl, 
                   null,
                   null,
                   null);
        this.refRcl = (ResultColumnList)refRcl;
        this.refTableName = (TableName)refTableName;

        this.refActionDeleteRule = ((int[])refActions)[0];
        this.refActionUpdateRule = ((int[])refActions)[1];

        this.grouping = ((Boolean)grouping).booleanValue();
    }

    // For DROP
    public void init(Object constraintName,
                     Object constraintType,
                     Object behavior,
                     Object grouping) {
        super.init(constraintName,
                   constraintType,
                   null,
                   null,
                   null,
                   null,
                   behavior,
                   ConstraintType.FOREIGN_KEY);
        this.grouping = ((Boolean)grouping).booleanValue();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        FKConstraintDefinitionNode other = (FKConstraintDefinitionNode)node;
        this.refTableName = (TableName)getNodeFactory().copyNode(other.refTableName,
                                                                 getParserContext());
        this.refRcl = (ResultColumnList)getNodeFactory().copyNode(other.refRcl,
                                                                  getParserContext());
        this.refActionDeleteRule = other.refActionDeleteRule;
        this.refActionUpdateRule = other.refActionUpdateRule;
    }

    public TableName getRefTableName() { 
        return refTableName; 
    }

    public ResultColumnList getRefResultColumnList() {
        return refRcl;
    }

    public boolean isGrouping() {
        return grouping;
    }
    
    public String toString() {
        return "refTable name : " + refTableName + "\n" +
            "grouping: " + grouping + "\n" + 
            super.toString();
    }
    

}
