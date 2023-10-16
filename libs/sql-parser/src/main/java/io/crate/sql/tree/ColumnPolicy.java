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

package io.crate.sql.tree;

import java.util.List;
import java.util.Locale;

import org.jetbrains.annotations.Nullable;

import io.crate.common.Booleans;


public enum ColumnPolicy {
    DYNAMIC {
        @Override
        public String toMappingValue() {
            return String.valueOf(true);
        }
    },
    STRICT {
        @Override
        public String toMappingValue() {
            return "strict";
        }
    },
    IGNORED {
        @Override
        public String toMappingValue() {
            return String.valueOf(false);
        }
    };

    public static final List<ColumnPolicy> VALUES = List.of(values());
    public static final String MAPPING_KEY = "dynamic";

    public String lowerCaseName() {
        return name().toLowerCase(Locale.ENGLISH);
    }

    public abstract String toMappingValue();

    public static ColumnPolicy fromMappingValue(@Nullable Object value) {
        if (value == null) {
            return DYNAMIC;
        }
        String str = value.toString();
        if (Booleans.isTrue(str)) {
            return DYNAMIC;
        }
        if (Booleans.isFalse(str)) {
            return IGNORED;
        }
        if (str.equalsIgnoreCase("strict")) {
            return STRICT;
        }
        throw new IllegalArgumentException("Invalid column policy: " + value);
    }

    public static ColumnPolicy of(String value) {
        switch (value.toLowerCase(Locale.ENGLISH)) {
            case "dynamic":
                return DYNAMIC;

            case "strict":
                return STRICT;

            case "ignored":
                return IGNORED;

            default:
                throw new IllegalArgumentException(
                    "Invalid column policy: " + value + " use one of [dynamic, strict, ignored]");
        }
    }
}
