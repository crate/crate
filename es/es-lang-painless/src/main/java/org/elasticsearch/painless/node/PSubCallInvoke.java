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
import org.elasticsearch.painless.lookup.PainlessMethod;

import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Represents a method call.
 */
final class PSubCallInvoke extends AExpression {

    private final PainlessMethod method;
    private final Class<?> box;
    private final List<AExpression> arguments;

    PSubCallInvoke(Location location, PainlessMethod method, Class<?> box, List<AExpression> arguments) {
        super(location);

        this.method = Objects.requireNonNull(method);
        this.box = box;
        this.arguments = Objects.requireNonNull(arguments);
    }

    @Override
    void extractVariables(Set<String> variables) {
        throw createError(new IllegalStateException("Illegal tree structure."));
    }

    @Override
    void analyze(Locals locals) {
        for (int argument = 0; argument < arguments.size(); ++argument) {
            AExpression expression = arguments.get(argument);

            expression.expected = method.typeParameters.get(argument);
            expression.internal = true;
            expression.analyze(locals);
            arguments.set(argument, expression.cast(locals));
        }

        statement = true;
        actual = method.returnType;
    }

    @Override
    void write(MethodWriter writer, Globals globals) {
        writer.writeDebugInfo(location);

        if (box.isPrimitive()) {
            writer.box(MethodWriter.getType(box));
        }

        for (AExpression argument : arguments) {
            argument.write(writer, globals);
        }

        writer.invokeMethodCall(method);
    }

    @Override
    public String toString() {
        return singleLineToStringWithOptionalArgs(arguments, prefix, method.javaMethod.getName());
    }
}
