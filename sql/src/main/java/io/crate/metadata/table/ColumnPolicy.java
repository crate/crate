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

package io.crate.metadata.table;

import org.elasticsearch.common.Booleans;

import javax.annotation.Nullable;
import java.util.Locale;

public enum ColumnPolicy {
    DYNAMIC(true),
    STRICT("strict"),
    IGNORED(false);

    public static final String ES_MAPPING_NAME = "dynamic";

    private Object mappingValue;

    private ColumnPolicy(Object mappingValue) {
        this.mappingValue = mappingValue;
    }

    public String value() {
        return this.name().toLowerCase(Locale.ENGLISH);
    }

    /**
     * get a column policy by its name (case insensitive)
     */
    public static ColumnPolicy byName(String name) {
        return ColumnPolicy.valueOf(name.toUpperCase(Locale.ENGLISH));
    }

    /**
     * get a column policy by its mapping value (true, false or 'strict')
     */
    public static ColumnPolicy of(@Nullable Object dynamic) {
        return of(String.valueOf(dynamic));
    }

    public static ColumnPolicy of(String dynamic) {
        if (Booleans.isTrue(dynamic)) {
            return DYNAMIC;
        }
        if (Booleans.isFalse(dynamic)) {
            return IGNORED;
        }
        if (dynamic.equalsIgnoreCase("strict")) {
            return STRICT;
        }
        return DYNAMIC;
    }

    /**
     * returns the value to be used in an ES index mapping
     * for the <code>dynamic</code> field
     */
    public Object mappingValue() {
        return mappingValue;
    }

}
