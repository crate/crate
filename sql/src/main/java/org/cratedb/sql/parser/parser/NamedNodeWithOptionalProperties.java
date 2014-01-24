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

/**
 * a node with a name and an optional list of unspecified properties.
 * Syntax:
 *
 *      name [ with (property1=value1, property2=value2, ... ) ]
 */
public class NamedNodeWithOptionalProperties extends QueryTreeNode {

    private String name;
    private GenericProperties properties = null;

    @Override
    public void init(Object name) throws StandardException {
        this.name = (String)name;
    }

    @Override
    public void init(Object name, Object properties) throws StandardException {
        this.name = (String)name;
        this.properties = (GenericProperties) properties;
    }

    public String getName() {
        return name;
    }

    public GenericProperties getProperties() {
        return properties;
    }

    @Override
    public String toString() {
        return this.name + super.toString();
    }

    @Override
    public void printSubNodes(int depth) {
        super.printSubNodes(depth);
        printLabel(depth, "Properties: ");
        if (properties != null) {
            properties.treePrint(depth+1);
        } else {
            debugPrint(formatNodeString("null", depth+1));
        }
    }

    @Override
    void acceptChildren(Visitor v) throws StandardException {
        super.acceptChildren(v);
        properties.accept(v);
    }
}
