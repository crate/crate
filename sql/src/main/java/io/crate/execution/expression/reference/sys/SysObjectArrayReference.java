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

package io.crate.execution.expression.reference.sys;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import io.crate.metadata.ReferenceImplementation;
import io.crate.execution.expression.reference.NestedObjectExpression;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;

public abstract class SysObjectArrayReference implements ReferenceImplementation<Object[]> {

    protected abstract List<NestedObjectExpression> getChildImplementations();

    @Override
    public ReferenceImplementation<Object[]> getChildImplementation(String name) {
        List<NestedObjectExpression> childImplementations = getChildImplementations();
        final Object[] values = new Object[childImplementations.size()];
        int i = 0;
        for (NestedObjectExpression sysObjectReference : childImplementations) {
            ReferenceImplementation<?> child = sysObjectReference.getChildImplementation(name);
            if (child != null) {
                Object value = child.value();
                values[i++] = value;
            } else {
                values[i++] = null;
            }
        }
        return () -> values;
    }

    @Override
    public Object[] value() {
        List<NestedObjectExpression> childImplementations = getChildImplementations();
        Object[] values = new Object[childImplementations.size()];
        int i = 0;
        for (NestedObjectExpression expression : childImplementations) {
            Map<String, Object> map = Maps.transformValues(expression.getChildImplementations(), new Function<ReferenceImplementation, Object>() {
                @Nullable
                @Override
                public Object apply(@Nullable ReferenceImplementation input) {
                    Object value = input.value();
                    if (value != null && value instanceof BytesRef) {
                        return ((BytesRef) value).utf8ToString();
                    } else {
                        return value;
                    }
                }
            });
            values[i++] = map;
        }
        return values;
    }
}
