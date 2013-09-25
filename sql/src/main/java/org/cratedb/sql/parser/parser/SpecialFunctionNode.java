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

   Derby - Class org.apache.derby.impl.sql.compile.SpecialFunctionNode

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

import java.sql.Types;

/**
         SpecialFunctionNode handles system SQL functions.
         A function value is either obtained by a method
         call off the LanguageConnectionContext or Activation.
         LanguageConnectionContext functions are state related to the connection.
         Activation functions are those related to the statement execution.

         Each SQL function takes no arguments and returns a SQLvalue.
         <P>
         Functions supported:
         <UL>
         <LI> USER
         <LI> CURRENT_USER
         <LI> CURRENT_ROLE
         <LI> SESSION_USER
         <LI> SYSTEM_USER
         <LI> CURRENT SCHEMA
         <LI> CURRENT ISOLATION
         <LI> IDENTITY_VAL_LOCAL

         </UL>


        <P>

         This node is used rather than some use of MethodCallNode for
         runtime performance. MethodCallNode does not provide a fast access
         to the current language connection or activatation, since it is geared
         towards user defined routines.


*/
public class SpecialFunctionNode extends ValueNode 
{
    /**
       Name of SQL function
    */
    String sqlName;

    /*
      print the non-node subfields
    */
    public String toString() {
        return "sqlName: " + sqlName + "\n" +
            super.toString();
    }
                
    protected boolean isEquivalent(ValueNode o) {
        if (isSameNodeType(o)) {
            SpecialFunctionNode other = (SpecialFunctionNode)o;
            return sqlName.equals(other.sqlName);
        }
        return false;
    }

}
