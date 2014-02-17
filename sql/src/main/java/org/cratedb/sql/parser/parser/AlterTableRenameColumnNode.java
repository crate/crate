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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

public class AlterTableRenameColumnNode extends TableElementNode
{
    private String oldName;     // old column name
    private String newName;     // new column name
    
    @Override
    public void init(Object oldN, Object newN)
    {
        oldName = (String) oldN;
        newName = (String) newN;
        super.init(oldName, ElementType.AT_RENAME_COLUMN);
    }
    
    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException
    {
        super.copyFrom(node);
        oldName = ((AlterTableRenameColumnNode)node).oldName;
        newName = ((AlterTableRenameColumnNode)node).newName;
    }
    
    public String newName()
    {
        return newName;
    }
}
