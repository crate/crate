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
import org.cratedb.sql.parser.types.TypeId;

/**
 * A CreateSequenceNode is the root of a QueryTree that
 * represents a CREATE SEQUENCE statement.
 */

public class CreateSequenceNode extends DDLStatementNode
{
    private TableName sequenceName;
    private DataTypeDescriptor dataType;
    private long initialValue;
    private long stepValue;
    private long maxValue;
    private long minValue;
    private boolean cycle;

    /**
     * Initializer for a CreateSequenceNode
     *
     * @param sequenceName The name of the new sequence
     * @param dataType Exact numeric type of the new sequence
     * @param initialValue Starting value
     * @param stepValue Increment amount
     * @param maxValue Largest value returned by the sequence generator
     * @param minValue Smallest value returned by the sequence generator
     * @param cycle True if the generator should wrap around, false otherwise
     *
     * @throws StandardException on error
     */
    public void init (Object sequenceName,
                      Object dataType,
                      Object initialValue,
                      Object stepValue,
                      Object maxValue,
                      Object minValue,
                      Object cycle) 
            throws StandardException {

        this.sequenceName = (TableName)sequenceName;
        initAndCheck(this.sequenceName);

        if (dataType != null) {
            this.dataType = (DataTypeDescriptor)dataType;
        } 
        else {
            this.dataType = DataTypeDescriptor.INTEGER;
        }

        this.stepValue = stepValue != null ? ((Long)stepValue).longValue() : 1;

        if (this.dataType.getTypeId().equals(TypeId.SMALLINT_ID)) {
            this.minValue = minValue != null ? ((Long)minValue).longValue() : Short.MIN_VALUE;
            this.maxValue = maxValue != null ? ((Long)maxValue).longValue() : Short.MAX_VALUE;
        } 
        else if (this.dataType.getTypeId().equals(TypeId.INTEGER_ID)) {
            this.minValue = minValue != null ? ((Long)minValue).longValue() : Integer.MIN_VALUE;
            this.maxValue = maxValue != null ? ((Long)maxValue).longValue() : Integer.MAX_VALUE;
        }
        else {
            this.minValue = minValue != null ? ((Long)minValue).longValue() : Long.MIN_VALUE;
            this.maxValue = maxValue != null ? ((Long)maxValue).longValue() : Long.MAX_VALUE;
        }

        if (initialValue != null) {
            this.initialValue = ((Long)initialValue).longValue();
        } 
        else {
            if (this.stepValue > 0) {
                this.initialValue = this.minValue;
            } 
            else {
                this.initialValue = this.maxValue;
            }
        }
        this.cycle = cycle != null ? ((Boolean)cycle).booleanValue() : Boolean.FALSE;

    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        CreateSequenceNode other = (CreateSequenceNode)node;
        this.sequenceName = (TableName)getNodeFactory().copyNode(other.sequenceName,
                                                                 getParserContext());
        this.dataType = other.dataType;
        this.initialValue = other.initialValue;
        this.stepValue = other.stepValue;
        this.maxValue = other.maxValue;
        this.minValue = other.minValue;
        this.cycle = other.cycle;
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return super.toString() +
            "sequenceName: " + sequenceName + "\n" +
            "initial value: " + initialValue + "\n" +
            "step value: " + stepValue + "\n" +
            "maxValue: " + maxValue + "\n" +
            "minValue:" + minValue + "\n" +
            "cycle: " + cycle + "\n";
    }

    public String statementToString() {
        return "CREATE SEQUENCE";
    }

    public final long getInitialValue() {
        return initialValue;
    }

    public final long getStepValue() {
        return stepValue;
    }

    public final long getMaxValue() {
        return maxValue;
    }

    public final long getMinValue() {
        return minValue;
    }

    public final boolean isCycle() {
        return cycle;
    }

}
