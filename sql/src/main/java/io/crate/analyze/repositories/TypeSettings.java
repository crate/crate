/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.analyze.repositories;

import com.google.common.collect.ImmutableSet;

import java.util.Set;

public class TypeSettings {
    private static final Set<String> GENERIC = ImmutableSet.of("max_restore_bytes_per_sec", "max_snapshot_bytes_per_sec");
    private final Set<String> required;
    private final Set<String> all;

    public TypeSettings(Set<String> required, Set<String> optional) {
        this.required = required;
        this.all = ImmutableSet.<String>builder().addAll(required).addAll(optional).addAll(GENERIC).build();
    }

    public Set<String> required() {
        return required;
    }

    public Set<String> all() {
        return all;
    }
}
