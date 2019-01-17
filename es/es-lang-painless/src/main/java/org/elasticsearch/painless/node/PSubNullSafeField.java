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
import org.objectweb.asm.Label;

import java.util.Set;

/**
 * Implements a field who's value is null if the prefix is null rather than throwing an NPE.
 */
public class PSubNullSafeField extends AStoreable {
    private AStoreable guarded;

    public PSubNullSafeField(Location location, AStoreable guarded) {
        super(location);
        this.guarded = guarded;
    }

    @Override
    void extractVariables(Set<String> variables) {
        guarded.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        if (write) {
            throw createError(new IllegalArgumentException("Can't write to null safe reference"));
        }
        guarded.read = read;
        guarded.analyze(locals);
        actual = guarded.actual;
        if (actual.isPrimitive()) {
            throw new IllegalArgumentException("Result of null safe operator must be nullable");
        }
    }


    @Override
    int accessElementCount() {
        return guarded.accessElementCount();
    }

    @Override
    boolean isDefOptimized() {
        return guarded.isDefOptimized();
    }

    @Override
    void updateActual(Class<?> actual) {
        guarded.updateActual(actual);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        Label end = new Label();
        writer.dup();
        writer.ifNull(end);
        guarded.write(writer, globals);
        writer.mark(end);
    }

    @Override
    void setup(MethodWriter writer, Globals globals) {
        throw createError(new IllegalArgumentException("Can't write to null safe field"));
    }

    @Override
    void load(MethodWriter writer, Globals globals) {
        throw createError(new IllegalArgumentException("Can't write to null safe field"));
    }

    @Override
    void store(MethodWriter writer, Globals globals) {
        throw createError(new IllegalArgumentException("Can't write to null safe field"));
    }

    @Override
    public String toString() {
        return singleLineToString(guarded);
    }
}
