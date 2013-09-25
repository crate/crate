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

   Derby - Class org.apache.derby.impl.sql.compile.RoutineDesignator

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

import org.cratedb.sql.parser.types.DataTypeDescriptor;

import java.util.List;

/**
 * This node represents a routine signature.
 */
class RoutineDesignator
{
    boolean isSpecific;
    TableName name; // TableName is a misnomer it is really just a schema qualified name
    boolean isFunction; // else a procedure
    /**
     * A list of DataTypeDescriptors
     * if null then the signature is not specified and this designator is ambiguous if there is
     * more than one function (procedure) with this name.
     */
    List<DataTypeDescriptor> paramTypeList;

    RoutineDesignator(boolean isSpecific,
                      TableName name,
                      boolean isFunction,
                      List<DataTypeDescriptor> paramTypeList) {
        this.isSpecific = isSpecific;
        this.name = name;
        this.isFunction = isFunction;
        this.paramTypeList = paramTypeList;
    }

}
