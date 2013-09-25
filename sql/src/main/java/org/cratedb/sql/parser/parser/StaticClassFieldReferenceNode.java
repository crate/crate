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

   Derby - Class org.apache.derby.impl.sql.compile.StaticClassFieldReferenceNode

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
 * A StaticClassFieldReferenceNode represents a Java static field reference from 
 * a Class (as opposed to an Object).    Field references can be 
 * made in DML (as expressions).
 *
 */

public final class StaticClassFieldReferenceNode extends JavaValueNode
{
    /*
    ** Name of the field.
    */
    private String fieldName;

    /* The class name */
    private String javaClassName;
    private boolean classNameDelimitedIdentifier;

    /**
     * Initializer for a StaticClassFieldReferenceNode
     *
     * @param javaClassName The class name
     * @param fieldName The field name
     */
    public void init(Object javaClassName, 
                     Object fieldName, 
                     Object classNameDelimitedIdentifier) {
        this.fieldName = (String)fieldName;
        this.javaClassName = (String)javaClassName;
        this.classNameDelimitedIdentifier = ((Boolean)classNameDelimitedIdentifier).booleanValue();
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        StaticClassFieldReferenceNode other = (StaticClassFieldReferenceNode)node;
        this.fieldName = other.fieldName;
        this.javaClassName = other.javaClassName;
        this.classNameDelimitedIdentifier = other.classNameDelimitedIdentifier;
    }

}
