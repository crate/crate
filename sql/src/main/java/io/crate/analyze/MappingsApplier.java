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

package io.crate.analyze;

import io.crate.data.Row;
import io.crate.sql.tree.Expression;

import java.util.Locale;
import java.util.Map;

public abstract class MappingsApplier {

    protected final String name;
    protected final String publicName;
    protected final Object defaultValue;

    public MappingsApplier(String name, String publicName, Object defaultValue) {
        this.name = name;
        this.publicName = publicName;
        this.defaultValue = defaultValue;
    }

    public abstract void apply(Map<String, Object> mappings, Row parameters, Expression expression);

    public void applyValue(Map<String, Object> mappings, Object value) {
        try {
            mappings.put(name, validate(value));
        } catch (IllegalArgumentException e) {
            throw invalidException(e);
        }
    }

    public Object validate(Object value) {
        return value;
    }

    public Object getDefault() {
        return defaultValue;
    }

    public IllegalArgumentException invalidException(Exception cause) {
        return new IllegalArgumentException(
            String.format(Locale.ENGLISH, "Invalid value for argument '%s'", publicName), cause);
    }

    public IllegalArgumentException invalidException() {
        return new IllegalArgumentException(
            String.format(Locale.ENGLISH, "Invalid value for argument '%s'", publicName));
    }

    @Override
    public String toString() {
        return "MappingsApplier{" + publicName + '}';
    }
}
