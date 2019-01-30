/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.painless.node;

import org.elasticsearch.painless.Globals;
import org.elasticsearch.painless.Locals;
import org.elasticsearch.painless.Location;
import org.elasticsearch.painless.MethodWriter;

import java.util.Objects;
import java.util.Set;

/**
 * Represents an array length field load.
 */
final class PSubArrayLength extends AStoreable {

    private final String type;
    private final String value;

    PSubArrayLength(Location location, String type, String value) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.value = Objects.requireNonNull(value);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        if ("length".equals(value)) {
            if (write) {
                throw createError(new IllegalArgumentException("Cannot write to read-only field [length] for an array."));
            }

            actual = int.class;
        } else {
            throw createError(new IllegalArgumentException("Field [" + value + "] does not exist for type [" + type + "]."));
        }
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);
        writer.arrayLength();
    }

    @Override
    int accessElementCount() {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    boolean isDefOptimized() {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    void updateActual(Class<?> actual) {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        throw new IllegalStateException("Illegal tree structure.");
    }

    @Override
    public String toString() {
        return singleLineToString(prefix);
    }
}
