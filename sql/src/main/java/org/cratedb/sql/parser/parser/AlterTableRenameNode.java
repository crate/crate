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

package org.cratedb.sql.parser.parser;

import org.cratedb.sql.parser.StandardException;

public class AlterTableRenameNode extends TableElementNode
{
    private TableName newName;
    
    @Override
    public void init(Object newTableName)
    {
        newName = (TableName)newTableName;
        super.init(newName.getFullTableName(), ElementType.AT_RENAME);
    }

    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException
    {
        super.copyFrom(node);
        
        newName = ((AlterTableRenameNode)node).newName;
    }
    
    public TableName newName()
    {
        return newName;
    }
}
