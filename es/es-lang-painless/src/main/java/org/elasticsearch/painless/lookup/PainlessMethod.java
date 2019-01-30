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

package org.elasticsearch.painless.lookup;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class PainlessMethod {

    public final Method javaMethod;
    public final Class<?> targetClass;
    public final Class<?> returnType;
    public final List<Class<?>> typeParameters;
    public final MethodHandle methodHandle;
    public final MethodType methodType;

    public PainlessMethod(Method javaMethod, Class<?> targetClass, Class<?> returnType, List<Class<?>> typeParameters,
            MethodHandle methodHandle, MethodType methodType) {

        this.javaMethod = javaMethod;
        this.targetClass = targetClass;
        this.returnType = returnType;
        this.typeParameters = Collections.unmodifiableList(typeParameters);
        this.methodHandle = methodHandle;
        this.methodType = methodType;
    }

    @Override
    public boolean equals(Object object) {
        if (this == object) {
            return true;
        }

        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        PainlessMethod that = (PainlessMethod)object;

        return Objects.equals(javaMethod, that.javaMethod) &&
                Objects.equals(targetClass, that.targetClass) &&
                Objects.equals(returnType, that.returnType) &&
                Objects.equals(typeParameters, that.typeParameters) &&
                Objects.equals(methodType, that.methodType);
    }

    @Override
    public int hashCode() {
        return Objects.hash(javaMethod, targetClass, returnType, typeParameters, methodType);
    }
}
