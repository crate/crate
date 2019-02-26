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

package io.crate.es.common.bytes;

import io.crate.es.common.util.BigArrays;
import io.crate.es.common.util.ByteArray;
import io.crate.es.common.lease.Releasable;
import io.crate.es.common.lease.Releasables;

/**
 * An extension to {@link PagedBytesReference} that requires releasing its content. This
 * class exists to make it explicit when a bytes reference needs to be released, and when not.
 */
public final class ReleasablePagedBytesReference extends PagedBytesReference implements Releasable {

    private final Releasable releasable;

    public ReleasablePagedBytesReference(BigArrays bigarrays, ByteArray byteArray, int length,
                                         Releasable releasable) {
        super(bigarrays, byteArray, length);
        this.releasable = releasable;
    }

    @Override
    public void close() {
        Releasables.close(releasable);
    }

}
