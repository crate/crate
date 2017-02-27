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


import io.crate.data.BatchProjector;

import javax.annotation.Nullable;

public interface Projector extends RowReceiver {

    void downstream(RowReceiver rowDownstreamHandle);

    /**
     * This method is used to get the downstream of the projector if users of this projector want to replace this instance
     * with a {@link io.crate.data.BatchProjector} implementation.
     *
     * @return the downstream row receiver of this projector
     */
    RowReceiver downstream();

    /**
     * Returns a {@link BatchProjector} object which implements the same semantics as this row receiver based
     * implementation if available, otherwise null.
     *
     * @return a batch projector or null.
     */
    @Nullable
    default BatchProjector batchProjectorImpl() {
        return null;
    }
}
