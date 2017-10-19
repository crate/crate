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

package io.crate.test;

import com.google.common.collect.ImmutableSet;
import org.apache.lucene.util.LuceneTestCase;
import org.codehaus.groovy.reflection.ClassInfo;

import java.lang.ref.SoftReference;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Set;

/**
 * COPIED FROM https://github.com/elastic/elasticsearch-groovy
 * <p>
 * <p>
 * {@code GroovyTestSanitizer} sanitizes the existing test infrastructure to work well with the Groovy runtime.
 * <ul>
 * <li>Modifies {@link LuceneTestCase} to add Groovy's {@link ClassInfo} type to the list (literally it's a
 * {@code Set}) of types that are ignored during the cleanup process.</li>
 * <li>This also adds the {@code SoftReference} to the list of types are ignored so that we can continue to support
 * the Grails release.</li>
 * </ul>
 * This should be invoked in a {@code static} block of every abstract test that uses the Elasticsearch test framework.
 * <pre>
 * static {
 *     assert GroovyTestSanitizer.groovySanitized
 * }
 * </pre>
 */
public class GroovyTestSanitizer {

    /**
     * This will thread safely trigger any necessary action to appropriately ignore {@code static} Groovy baggage
     * during test cleanup. This is necessary to avoid test failures due to automatic static field monitoring for
     * improper test code cleanup (and therefore JVM waste and theoretically slower tests).
     * <pre>
     * static {*     assert GroovyTestSanitizer.groovySanitized
     * }
     * </pre>
     *
     * @return {@code true} to indicate that tests can be safely executed. Expected to always be {@code true}.
     */
    public static boolean isGroovySanitized() {
        // This forces the class loader to load the inner type, thereby triggering its static block.
        // The JVM guarantees that this will only happen once, so we can lock-free do this as many times as we want
        //  and the static block will only ever be executed once.
        return GroovyTestSanitizerSingleton.load();
    }

    /**
     * An inner class is used to force the JVM to perform its Classloader magic. This allows us to use it as though
     * it's locked without ever using locks ourselves.
     * <p>
     * It can be used as many times as necessary, but it will only be invoked once.
     */
    private static class GroovyTestSanitizerSingleton {
        /**
         * Currently, this modifies {@link LuceneTestCase} to add {@link ClassInfo} to the set of known
         * static leak types to be ignored.
         */
        static {
            // Types of static fields that are added by Groovy at runtime
            // - SoftReferences are the $callSiteArrays and they should be removed from the safe list once we only
            //   support running with invokedynamic support
            ImmutableSet<String> safeGroovyTypes = ImmutableSet.of(ClassInfo.class.getName(), SoftReference.class.getName());

            try {
                // this corresponds to a Set<String>
                Field field = LuceneTestCase.class.getDeclaredField("STATIC_LEAK_IGNORED_TYPES");

                // the field is private static, so this allows us to mess with it
                field.setAccessible(true);

                // the field is also final, so we need to remove that
                Field modifiersField = Field.class.getDeclaredField("modifiers");
                modifiersField.setAccessible(true);
                modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);

                // read the field
                Set<String> staticLeakIgnoreTypes = (Set<String>) field.get(null);
                // replace the field: add our own class to it, then replace it
                field.set(null, ImmutableSet.builder().addAll(staticLeakIgnoreTypes).addAll(safeGroovyTypes).build());

                // reset it as final
                modifiersField.setInt(field, field.getModifiers() | Modifier.FINAL);
                modifiersField.setAccessible(false);

                // reset it as private
                field.setAccessible(false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        /**
         * An arbitrary method to allow the outer class to easily trigger the {@code static} block.
         */
        static boolean load() {
            return true;
        }
    }
}
