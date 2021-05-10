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

package io.crate.common;

/**
 * An operation that accepts three input arguments and returns no result.
 *
 * @param <K> type of the first argument
 * @param <V> type of the second argument
 * @param <S> type of the third argument
 */
public interface TriConsumer<K, V, S> {

    /**
     * Performs the operation given the specified arguments.
     * @param k the first input argument
     * @param v the second input argument
     * @param s the third input argument
     */
    void accept(K k, V v, S s);
}
