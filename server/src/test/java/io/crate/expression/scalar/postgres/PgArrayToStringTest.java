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

package io.crate.expression.scalar.postgres;

import io.crate.expression.scalar.ArrayToStringFunction;
import io.crate.expression.scalar.ScalarTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;

public class PgArrayToStringTest extends ScalarTestCase {
    @Test
    public void testPgArrayToStringWithFQNFunctionName() throws Exception {
        assertEvaluate("pg_catalog.array_to_string([1, 2, 3], ', ')", "1, 2, 3");
    }

    @Test
    public void testPgArrayToString() throws Exception {
        // PgArrayToString must be a subclass of ArrayToStringFunction.
        // It may only have a constructor and a static register method

        Assert.assertTrue(PgArrayToString.class.getSuperclass() == ArrayToStringFunction.class);

        Constructor<?>[] constructors = PgArrayToString.class.getConstructors();
        Assert.assertTrue(constructors.length == 1);

        Method[] methods = PgArrayToString.class.getMethods();
        for(Method method: methods) {
            Class<?> declaringClass = method.getDeclaringClass();
            Assert.assertTrue ((declaringClass != PgArrayToString.class) ||
                (declaringClass == PgArrayToString.class && method.getName() == "register"));
        }
    }
}
