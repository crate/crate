/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.expression.reference;

import io.crate.common.collections.Maps;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.NestableInput;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.UnaryOperator;

public final class MapLookupByPathExpression<T> implements NestableCollectExpression<T, Object> {

    private final Function<T, Map<String, Object>> getMap;
    private final List<String> path;
    private final UnaryOperator<Object> castResultValue;
    private Object value;

    public MapLookupByPathExpression(Function<T, Map<String, Object>> getMap,
                                     List<String> path,
                                     UnaryOperator<Object> castResultValue) {
        this.getMap = getMap;
        this.path = path;
        this.castResultValue = castResultValue;
    }

    @Override
    public void setNextRow(T row) {
        if (path.isEmpty()) {
            value = getMap.apply(row);
        } else {
            value = castResultValue.apply(Maps.getByPath(getMap.apply(row), path));
        }
    }

    @Override
    public Object value() {
        return value;
    }

    @Override
    public NestableInput<?> getChild(String name) {
        ArrayList<String> newPath = new ArrayList<>(path.size() + 1);
        newPath.addAll(path);
        newPath.add(name);
        return new MapLookupByPathExpression<>(getMap, newPath, castResultValue);
    }

}
