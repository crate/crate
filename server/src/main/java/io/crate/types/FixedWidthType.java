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

package io.crate.types;

/**
 * A type that has a fixed size for every value
 */
public interface FixedWidthType {

    /**
     * The fixed amount of memory a value object instance of type t requires.
     * (t is the type described by our DataType interface or something that implements FixedWidthType)
     * <p>
     * <p>
     * Implementations here may not be 100% accurate because sizes may vary between JVM implementations
     * and then there is also stuff like padding and other JVM magic.
     * <p>
     * See also:
     * https://blogs.oracle.com/jrose/entry/fixnums_in_the_vm
     * http://www.javaworld.com/article/2077496/testing-debugging/java-tip-130--do-you-know-your-data-size-.html
     */
    int fixedSize();
}
