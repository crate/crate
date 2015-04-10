/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation;

import io.crate.core.collections.Row;

/**
 * A downstream handling rows
 */
public interface RowDownstreamHandle {


    /**
     * Feed the downstream with the next input row.
     * If the downstream does not need any more rows, it returns <code>false</code>,
     * <code>true</code> otherwise.
     *
     * This method must be thread safe.
     *
     * @return false if the downstream does not need any more rows, true otherwise.
     */
    boolean setNextRow(Row row);

    /**
     * Called from the upstream to indicate that all rows are sent.
     */
    void finish();

    /**
     * Is called from the upstream in case of a failure.
     */
    void fail(Throwable throwable);

}
