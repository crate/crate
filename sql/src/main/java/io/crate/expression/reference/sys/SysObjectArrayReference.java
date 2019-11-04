/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.expression.reference.sys;

import com.google.common.collect.Maps;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;
import io.crate.expression.reference.NestedObjectExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class SysObjectArrayReference implements NestableInput<List<Object>> {

    protected abstract List<NestedObjectExpression> getChildImplementations();

    @Override
    public NestableInput<List<Object>> getChild(String name) {
        List<NestedObjectExpression> childImplementations = getChildImplementations();
        final ArrayList<Object> values = new ArrayList<>(childImplementations.size());
        for (NestedObjectExpression sysObjectReference : childImplementations) {
            NestableInput<?> child = sysObjectReference.getChild(name);
            if (child != null) {
                Object value = child.value();
                values.add(value);
            } else {
                values.add(null);
            }
        }
        return NestableCollectExpression.constant(values);
    }

    @Override
    public List<Object> value() {
        List<NestedObjectExpression> childImplementations = getChildImplementations();
        ArrayList<Object> values = new ArrayList<>(childImplementations.size());
        for (NestedObjectExpression expression : childImplementations) {
            Map<String, Object> map = Maps.transformValues(expression.getChildImplementations(), NestableInput::value);
            values.add(map);
        }
        return values;
    }
}
