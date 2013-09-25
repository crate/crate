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
   Derby - Class org.apache.derby.impl.sql.compile.RowNumberFunctionNode

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
import org.cratedb.sql.parser.types.TypeId;

import java.sql.Types;

/**
 * Class that represents a call to the ROW_NUMBER() window function.
 */
public final class RowNumberFunctionNode extends WindowFunctionNode
{

    /**
     * Initializer. QueryTreeNode override.
     *
     * @param arg1 null (Operand)
     * @param arg2 The window definition or reference
     *
     * @exception StandardException
     */
    public void init(Object arg1, Object arg2) throws StandardException {
        super.init(arg1, "ROW_NUMBER", arg2);
        setType(TypeId.getBuiltInTypeId(Types.BIGINT),
                TypeId.LONGINT_PRECISION,
                TypeId.LONGINT_SCALE,
                false,
                TypeId.LONGINT_MAXWIDTH);
    }

}
