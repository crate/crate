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
import org.cratedb.sql.parser.types.DataTypeDescriptor;

/**
 * Crate Object Type Column Definition with optional subcolumns and an ObjectType
 * to define its behaviour
 */
public class ObjectColumnDefinitionNode extends ColumnDefinitionNode {
    private ObjectType objectType = ObjectType.DYNAMIC;
    private TableElementList subColumns = null;

    @Override
    public void init(Object name, Object objectType, Object subColumns) throws StandardException {
        super.init(name, null, DataTypeDescriptor.OBJECT, null);
        this.objectType = (ObjectType)objectType;
        this.subColumns = (TableElementList)subColumns;
    }

    public TableElementList subColumns() {
        return subColumns;
    }

    public ObjectType objectType() {
        return objectType;
    }

    @Override
    public void copyFrom(QueryTreeNode node) throws StandardException {
        super.copyFrom(node);
        ObjectColumnDefinitionNode other = (ObjectColumnDefinitionNode)node;
        this.objectType = other.objectType;
        this.subColumns = (TableElementList) getNodeFactory().copyNode(other.subColumns, getParserContext());
    }

    @Override
    public String toString() {
        return super.toString() +
                "objectType: " + objectType().name() + "\n";
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        if (subColumns!=null) {
            printLabel(depth, "subColumns: ");
            subColumns.treePrint(depth+1);
        }
    }
}
