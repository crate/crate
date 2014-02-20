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
 * A StaticMethodCallNode represents a static method call from a Class
 * (as opposed to from an Object).

     For a procedure the call requires that the arguments be ? parameters.
     The parameter is *logically* passed into the method call a number of different ways.

     <P>
     For a application call like CALL MYPROC(?) the logically Java method call is
     (in psuedo Java/SQL code) (examples with CHAR(10) parameter)
     <BR>
     Fixed length IN parameters - com.acme.MyProcedureMethod(?)
     <BR>
     Variable length IN parameters - com.acme.MyProcedureMethod(CAST (? AS CHAR(10))
     <BR>
     Fixed length INOUT parameter -
        String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
     <BR>
     Variable length INOUT parameter -
        String[] holder = new String[] {CAST (? AS CHAR(10)}; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))

     <BR>
     Fixed length OUT parameter -
        String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = holder[0]

     <BR>
     Variable length INOUT parameter -
        String[] holder = new String[1]; com.acme.MyProcedureMethod(holder); ? = CAST (holder[0] AS CHAR(10))


        <P>
    For static method calls there is no pre-definition of an IN or INOUT parameter, so a call to CallableStatement.registerOutParameter()
    makes the parameter an INOUT parameter, provided:
        - the parameter is passed directly to the method call (no casts or expressions).
        - the method's parameter type is a Java array type.

        Since this is a dynmaic decision we compile in code to take both paths, based upon a boolean isINOUT which is dervied from the
    ParameterValueSet. Code is logically (only single parameter String[] shown here). Note, no casts can exist here.

    boolean isINOUT = getParameterValueSet().getParameterMode(0) == PARAMETER_IN_OUT;
    if (isINOUT) {
        String[] holder = new String[] {?}; com.acme.MyProcedureMethod(holder); ? = holder[0]
         
    } else {
        com.acme.MyProcedureMethod(?)
    }

 *
 */
public class StaticMethodCallNode extends MethodCallNode
{
    private TableName procedureName;

    /**
     * Intializer for a NonStaticMethodCallNode
     *
     * @param methodName The name of the method to call
     * @param javaClassName The name of the java class that the static method belongs to.
     */
    public void init(Object methodName, Object javaClassName) {
        if (methodName instanceof String)
            init(methodName);
        else {
            procedureName = (TableName)methodName;
            init(procedureName.getTableName());
        }

        this.javaClassName = (String)javaClassName;
    }

    public TableName getProcedureName() {
        return procedureName;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        StaticMethodCallNode other = (StaticMethodCallNode)node;
        this.procedureName = (TableName)getNodeFactory().copyNode(other.procedureName,
                                                                  getParserContext());
    }

    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */

    public String toString() {
        return "javaClassName: " +
            (javaClassName != null ? javaClassName : "null") + "\n" +
            super.toString();
    }

}
