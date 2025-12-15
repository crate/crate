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

package io.crate.expression;

import io.crate.data.Input;

import org.jspecify.annotations.Nullable;
import java.util.List;

public interface NestableInput<T> extends Input<T> {

    /**
     * Returns an implementation for a child.
     *
     * @param name The name of the child
     * @return an implementation for the child or null if not applicable or if there is no child available
     * with the given name
     */
    @Nullable
    default NestableInput<?> getChild(String name) {
        return null;
    }

    @Nullable
    static NestableInput<?> getChildByPath(NestableInput<?> impl, List<String> path) {
        for (int i = 0; i < path.size(); i++) {
            impl = impl.getChild(path.get(i));
            if (impl == null) {
                return null;
            }
        }
        return impl;
    }
}
