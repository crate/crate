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

   Derby - Class org.apache.derby.impl.sql.compile.TablePrivilegesNode

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
 * This class represents a set of privileges on one table.
 */
public class TablePrivilegesNode extends QueryTreeNode
{
    // Action types
    // TODO: Could be enum, but used as array index below.
    public static final int SELECT_ACTION = 0;
    public static final int DELETE_ACTION = 1;
    public static final int INSERT_ACTION = 2;
    public static final int UPDATE_ACTION = 3;
    public static final int REFERENCES_ACTION = 4;
    public static final int TRIGGER_ACTION = 5;
    public static final int ACTION_COUNT = 6;

    private boolean[] actionAllowed = new boolean[ACTION_COUNT];
    private ResultColumnList[] columnLists = new ResultColumnList[ACTION_COUNT];

    /**
     * Add all actions
     */
    public void addAll() {
        for (int i = 0; i < ACTION_COUNT; i++) {
            actionAllowed[i] = true;
            columnLists[i] = null;
        }
    }

    /**
     * Add one action to the privileges for this table
     *
     * @param action The action type
     * @param privilegeColumnList The set of privilege columns. Null for all columns
     *
     * @exception StandardException standard error policy.
     */
    public void addAction(int action, ResultColumnList privilegeColumnList) {
        actionAllowed[action] = true;
        if (privilegeColumnList == null)
            columnLists[action] = null;
        else if (columnLists[action] == null)
            columnLists[action] = privilegeColumnList;
        else
            columnLists[action].appendResultColumns(privilegeColumnList, false);
    }

}
