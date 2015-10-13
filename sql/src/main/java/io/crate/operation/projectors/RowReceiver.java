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

package io.crate.operation.projectors;

import io.crate.core.collections.Row;
import io.crate.jobs.ExecutionState;
import io.crate.operation.RowUpstream;

import java.util.Set;

public interface RowReceiver {

    /**
     * Feed the downstream with the next input row.
     * If the downstream does not need any more rows, it returns <code>false</code>,
     * <code>true</code> otherwise.
     *
     * Any upstream who calls this methods must call {@link #setUpstream(RowUpstream)} exactly once
     * so that the receiver is able to call pause/resume on its upstream.
     *
     * {@link #finish()} and {@link #fail(Throwable)} may be called without calling setUpstream if there are no rows.
     *
     * @param row the next row - the row is usually a shared object and the instances content change after the
     *            setNextRow call.
     * @return false if the downstream does not need any more rows, true otherwise.
     */
    boolean setNextRow(Row row);

    /**
     * Called from the upstream to indicate that all rows are sent.
     *
     * NOTE: This method must not throw any exceptions!
     */
    void finish();

    /**
     * Is called from the upstream in case of a failure.
     * @param throwable the cause of the fail
     *
     * NOTE: This method must not throw any exceptions!
     */
    void fail(Throwable throwable);

    /**
     * prepares / starts the RowReceiver, after this call it must be ready to receive rows
     */
    void prepare(ExecutionState executionState);

    /**
     * an RowUpstream who wants to call {@link #setNextRow(Row)} calls setUpstream before he starts sending rows
     * so that the RowReceiver can call pause/resume on the upstream.
     */
    void setUpstream(RowUpstream rowUpstream);


    /**
     * specifies which requirements a downstream requires from an upstream in order to work correctly.
     *
     * This can be used to switch to optimized implementations if something isn't/is requirement
     */
    Set<Requirement> requirements();
}
