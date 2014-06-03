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

package io.crate.operation.reference.sys;

import com.google.common.base.Function;
import com.google.common.collect.Maps;
import io.crate.metadata.ReferenceImplementation;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.sys.SysExpression;

import javax.annotation.Nullable;
import java.util.*;

public abstract class SysArrayObjectReference<ChildType> extends SysExpression<List<Map<String, ChildType>>>
        implements ReferenceImplementation {

    protected final List<SysObjectReference<ChildType>> childImplementations = new ArrayList<>();

    @Override
    public SysExpression<List<ChildType>> getChildImplementation(String name) {
        final List<ChildType> list = new ArrayList<>(childImplementations.size());
        ReferenceInfo info = null;
        for (SysObjectReference<ChildType> sysObjectReference : childImplementations) {
            SysExpression<ChildType> child = sysObjectReference.getChildImplementation(name);
            if (child != null) {
                if (info == null) {
                    info = child.info();
                }
                list.add(child.value());
            }
        }
        final ReferenceInfo infoFinal = info;
        SysExpression<List<ChildType>> sysExpression = new SysExpression<List<ChildType>>() {
            @Override
            public List<ChildType> value() {
                return Collections.unmodifiableList(list);
            }

            @Override
            public ReferenceInfo info() {
                return infoFinal;
            }
        };
        return sysExpression;
    }

    @Override
    public List<Map<String, ChildType>> value() {
        List<Map<String, ChildType>> list = new ArrayList<>(childImplementations.size());
        for (SysObjectReference<ChildType> expression : childImplementations) {
            Map<String, ChildType> map = Maps.transformValues(expression.childImplementations, new Function<SysExpression<ChildType>, ChildType>() {
                @Nullable
                @Override
                public ChildType apply(@Nullable SysExpression<ChildType> input) {
                    return input.value();
                }
            });
            list.add(map);
        }
        return Collections.unmodifiableList(list);
    }
}
