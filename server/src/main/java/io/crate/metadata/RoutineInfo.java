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

package io.crate.metadata;

import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

@NullMarked
public record RoutineInfo(String name,
                          String type,
                          @Nullable String schema,
                          @Nullable String specificName,
                          @Nullable String definition,
                          @Nullable String body,
                          @Nullable String dataType,
                          boolean isDeterministic) {

    public RoutineInfo(String name, String type) {
        this(name, type, null, null, null, null, null, false);
    }

    public RoutineInfo(String name, String type, String definition) {
        this(name, type, null, null, definition, null, null, false);
    }
}
