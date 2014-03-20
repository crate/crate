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

package io.crate.operation.reference.sys;

import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.sys.SysExpression;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public abstract class SysObjectReference<ChildType> extends SysExpression<Map<String, ChildType>>
        implements ReferenceImplementation {

    protected final Map<String, SysExpression<ChildType>> childImplementations = new HashMap<>();

    @Override
    public SysExpression<ChildType> getChildImplementation(String name) {
        return childImplementations.get(name);
    }

    @Override
    public Map<String, ChildType> value() {
        Map<String, ChildType> map = new HashMap<>();
        for (Map.Entry<String, SysExpression<ChildType>> e : childImplementations.entrySet()) {
            map.put(e.getKey(), e.getValue().value());
        }
        return Collections.unmodifiableMap(map);
    }
}
