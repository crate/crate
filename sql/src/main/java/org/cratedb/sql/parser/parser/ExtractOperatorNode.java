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

   Derby - Class org.apache.derby.impl.sql.compile.ExtractOperatorNode

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
 * This node represents a unary extract operator, used to extract
 * a field from a date/time. The field value is returned as an integer.
 *
 */
public class ExtractOperatorNode extends UnaryOperatorNode 
{
    public static enum Field {
        YEAR("YEAR", "year"),
        MONTH("MONTH", "month"),
        DAY("DAY", "day"),
        HOUR("HOUR", "hour"),
        MINUTE("MINUTE", "minute"),
        SECOND("SECOND", "second");

        String fieldName, fieldMethod;

        Field(String fieldName, String fieldMethod) {
            this.fieldName = fieldName;
            this.fieldMethod = fieldMethod;
        }
    }

    private Field extractField;

    /**
     * Initializer for a ExtractOperatorNode
     *
     * @param field     The field to extract
     * @param operand The operand
     */
    public void init(Object field, Object operand) throws StandardException {
        extractField = (Field)field;
        super.init(operand,
                   "EXTRACT "+ extractField.fieldName,
                   extractField.fieldMethod);
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        ExtractOperatorNode other = (ExtractOperatorNode)node;
        this.extractField = other.extractField;
    }

    public String toString() {
        return "fieldName: " + extractField.fieldName + "\n" +
            super.toString();
    }

}
