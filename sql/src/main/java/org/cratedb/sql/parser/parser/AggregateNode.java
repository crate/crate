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
 * An Aggregate Node is a node that reprsents a set function/aggregate.
 * It used for all system aggregates as well as user defined aggregates.
 *
 */

public class AggregateNode extends UnaryOperatorNode
{
    private String aggregateName;
    private String aggregateDefinitionClassName;
    private boolean distinct;

    /**
     * Intializer.  Used for user defined and internally defined aggregates.
     * Called when binding a StaticMethodNode that we realize is an aggregate.
     *
     * @param operand   the value expression for the aggregate
     * @param uadClass  the class name for user aggregate definition for the aggregate
     *                  or internal aggregate type.
     * @param distinct  boolean indicating whether this is distinct
     *                  or not.
     * @param aggregateName the name of the aggregate from the user's perspective,
     *                      e.g. MAX
     *
     * @exception StandardException on error
     */
    public void init(Object operand,
                     Object uadClass,
                     Object distinct,
                     Object aggregateName) 
            throws StandardException {
        super.init(operand);
        this.aggregateDefinitionClassName = (String)uadClass;
        this.aggregateName = (String)aggregateName;
        this.distinct = ((Boolean)distinct).booleanValue();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        AggregateNode other = (AggregateNode)node;
        this.aggregateDefinitionClassName = other.aggregateDefinitionClassName;
        this.aggregateName = other.aggregateName;
        this.distinct = other.distinct;
    }

    /**
     * Get the name of the aggregate.
     *
     * @return the aggregate name
     */
    public String getAggregateName() {
        return aggregateName;
    }

    /**
     * Indicate whether this aggregate is distinct or not.
     *
     * @return true/false
     */
    public boolean isDistinct() {
        return distinct;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "aggregateName: " + aggregateName + "\n" +
            super.toString();
    }

}
