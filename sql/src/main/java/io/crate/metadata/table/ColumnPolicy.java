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

import java.util.Locale;

public enum ColumnPolicy {
    DYNAMIC(true),
    STRICT("strict"),
    IGNORED(false);

    private Object mappingValue;

    private ColumnPolicy(Object mappingValue) {
        this.mappingValue = mappingValue;
    }

    public String value() {
        return this.name().toLowerCase(Locale.ENGLISH);
    }

    public static ColumnPolicy byName(String name) {
        return ColumnPolicy.valueOf(name.toUpperCase(Locale.ENGLISH));
    }

    /**
     * returns the value to be used in an ES index mapping
     * for the <code>dynamic</code> field
     */
    public Object mappingValue() {
        return mappingValue;
    }

}
