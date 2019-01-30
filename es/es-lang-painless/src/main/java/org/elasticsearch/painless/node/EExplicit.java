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
 * Represents an explicit cast.
 */
public final class EExplicit extends AExpression {

    private final String type;
    private AExpression child;

    public EExplicit(Location location, String type, AExpression child) {
        super(location);

        this.type = Objects.requireNonNull(type);
        this.child = Objects.requireNonNull(child);
    }

    @Override
    void extractVariables(Set<String> variables) {
        child.extractVariables(variables);
    }

    @Override
    void analyze(Locals locals) {
        actual = locals.getPainlessLookup().canonicalTypeNameToType(type);

        if (actual == null) {
            throw createError(new IllegalArgumentException("Not a type [" + type + "]."));
        }

        child.expected = actual;
        child.explicit = true;
        child.analyze(locals);
        child = child.cast(locals);
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    AExpression cast(Locals locals) {
        child.expected = expected;
        child.explicit = explicit;
        child.internal = internal;

        return child.cast(locals);
    }

    @Override
    public String toString() {
        return singleLineToString(type, child);
    }
}
