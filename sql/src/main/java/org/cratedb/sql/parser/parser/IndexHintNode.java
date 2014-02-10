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

import java.util.List;

/**
 * MySQL's index hint.
 */
public class IndexHintNode extends QueryTreeNode
{
    public static enum HintType {
        USE, IGNORE, FORCE
    }
    
    public static enum HintScope {
        JOIN, ORDER_BY, GROUP_BY
    }

    private HintType hintType;
    private HintScope hintScope;
    private List<String> indexes;

    public void init(Object hintType,
                     Object hintScope,
                     Object indexes)
    {
        this.hintType = (HintType)hintType;
        this.hintScope = (HintScope)hintScope;
        this.indexes = (List<String>)indexes;
    }

    public HintType getHintType() {
        return hintType;
    }
    public HintScope getHintScope() {
        return hintScope;
    }
    public List<String> getIndexes() {
        return indexes;
    }

    /**
     * Fill this node with a deep copy of the given node.
     */
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);

        IndexHintNode other = (IndexHintNode)node;
        this.hintType = other.hintType;
        this.hintScope = other.hintScope;
        this.indexes = other.indexes;
    }
    
    /**
     * Convert this object to a String.  See comments in QueryTreeNode.java
     * for how this should be done for tree printing.
     *
     * @return This object as a String
     */
    public String toString() {
        return "hintType: " + hintType + "\n" +
            "hintScope: " + hintScope + "\n" +
            "indexes: " + indexes + "\n" +
            super.toString();
    }

}
