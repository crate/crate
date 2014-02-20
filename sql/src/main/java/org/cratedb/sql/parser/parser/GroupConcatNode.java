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

public class GroupConcatNode extends AggregateNode
{
    private String sep;
    private OrderByList orderCols;
    
    @Override
    public void init(Object value,
                     Object aggClass,
                     Object distinct,
                     Object aggName,
                     Object orderCols,
                     Object sep)
            throws StandardException
    {
        super.init(value,
                  aggClass,
                  distinct,
                  aggName);
        
        this.orderCols = (OrderByList) orderCols;
        this.sep = (String) sep;
    }
    
    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException
    {
        super.copyFrom(node);
        
        GroupConcatNode other = (GroupConcatNode) node;
        this.sep = other.sep;
        this.orderCols = (OrderByList) getNodeFactory().copyNode(other.orderCols,
                                                   getParserContext());
    }
    
    @Override
    void acceptChildren(Visitor v) throws StandardException
    {
        super.acceptChildren(v);
        
        if (orderCols != null)
            orderCols.acceptChildren(v);
    }

     /**
     * @inheritDoc
     */
    @Override
    protected boolean isEquivalent(ValueNode o) throws StandardException
    {
        if (!isSameNodeType(o))
            return false;
        
        GroupConcatNode other = (GroupConcatNode) o;
        
        return  this.sep.equals(other.sep)
             && this.orderCols.equals(other.orderCols);
    }

    @Override
    public String toString()
    {
        return super.toString() + 
               "\nseparator: " + sep +
               "\norderyByList: "+ orderCols;
                
    }
    
    public String getSeparator()
    {
        return sep;
    }
    
    public OrderByList getOrderBy()
    {
        return orderCols;
    }
}
