/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.operation.projectors;

import io.crate.operation.ProjectorUpstream;

/**
 * executing a Projection
 */
public interface Projector extends ProjectorUpstream {

    /**
     * initialize anything needed for proper projecting the projection
     */
    public void startProjection();

    /**
     * feed this Projector with the next input row.
     * If this projection does not need any more rows, it returns <code>false</code>,
     * <code>true</code> otherwise.
     *
     * This method must be thread safe.
     *
     * @return false if this projection does not need any more rows, true otherwise.
     */
    public boolean setNextRow(Object ... row);

    public void registerUpstream(ProjectorUpstream upstream);

    /**
     * Has to be called from upstream projectors to indicate that they sent all rows.
     * After this has been called a upstream projector must not send any more rows to the downstream.
     */
    public void upstreamFinished();

    /**
     * Has to be called by the upstream if an exception occurred.
     * Should be called once the upstream is finished with any other concurrently running actions.
     */
    public void upstreamFailed(Throwable throwable);
}
