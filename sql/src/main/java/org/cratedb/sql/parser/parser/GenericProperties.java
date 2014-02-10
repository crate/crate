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

import java.util.HashMap;
import java.util.Map;

public class GenericProperties extends QueryTreeNode {

    private final Map<String, QueryTreeNode> keyValues = new HashMap<>();

    public void init() {}

    @Override
    public void init(Object properties) throws StandardException {
        if (properties != null) {
            this.copyFrom((QueryTreeNode)properties);
        }

    }

    @Override
    public void copyFrom(QueryTreeNode other) throws StandardException {
        super.copyFrom(other);
        GenericProperties properties = (GenericProperties) other;
        this.keyValues.clear();
        if (properties.hasProperties()) {
            for (Map.Entry<String, QueryTreeNode> entry : properties.iterator()) {
                this.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public void put(String key, QueryTreeNode value) {
        keyValues.put(key, value);
    }

    public QueryTreeNode get(String key) {
        return keyValues.get(key);
    }


    public Iterable<Map.Entry<String, QueryTreeNode>> iterator() {
        return keyValues.entrySet();
    }
    public boolean hasProperties() {
        return this.keyValues.size() > 0;
    }

    @Override
    public String toString() {
        return keyValues.toString();
    }
}
