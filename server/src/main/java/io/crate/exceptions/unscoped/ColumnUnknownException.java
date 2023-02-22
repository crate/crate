/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
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

package io.crate.exceptions.unscoped;


import io.crate.exceptions.ResourceUnknownException;
import io.crate.exceptions.UnscopedException;

public class ColumnUnknownException extends RuntimeException implements ResourceUnknownException, UnscopedException {

    /**
     * <code>
     * ex) select '{"x":10}'::object['y'];<br>
     * ColumnUnknownException[The object `{x=10}` does not contain the key `y`]<br>
     * // The column is unnamed and there is no associated table that requires permission checks. This is the only usage currently.
     * </code>
     */
    public static ColumnUnknownException columnUnknownExceptionFromUnknownRelation(String message) {
        return new ColumnUnknownException(message);
    }

    private ColumnUnknownException(String message) {
        super(message);
    }
}
