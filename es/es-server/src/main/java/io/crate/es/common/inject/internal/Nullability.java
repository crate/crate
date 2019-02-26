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

package io.crate.es.common.inject.internal;

import java.lang.annotation.Annotation;

/**
 * Whether a member supports null values injected.
 * <p>Support for {@code Nullable} annotations in Guice is loose.
 * Any annotation type whose simplename is "Nullable" is sufficient to indicate
 * support for null values injected.
 * <p>This allows support for JSR-305's
 * <a href="http://groups.google.com/group/jsr-305/web/proposed-annotations">
 * javax.annotation.meta.Nullable</a> annotation and IntelliJ IDEA's
 * <a href="http://www.jetbrains.com/idea/documentation/howto.html">
 * org.jetbrains.annotations.Nullable</a>.
 *
 * @author jessewilson@google.com (Jesse Wilson)
 */
public class Nullability {
    private Nullability() {
    }

    public static boolean allowsNull(Annotation[] annotations) {
        for (Annotation a : annotations) {
            if ("Nullable".equals(a.annotationType().getSimpleName())) {
                return true;
            }
        }
        return false;
    }
}
