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

package org.elasticsearch.indices.recovery;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import java.io.IOException;

public class BlobRecoveryDeleteRequest extends BlobRecoveryRequest {

    public final BytesReference[] digests;

    public BlobRecoveryDeleteRequest(long recoveryId, BytesArray[] digests) {
        super(recoveryId);
        this.digests = digests;
    }

    public BlobRecoveryDeleteRequest(StreamInput in) throws IOException {
        super(in);
        digests = new BytesReference[in.readVInt()];
        for (int i = 0; i < digests.length; i++) {
            digests[i] = in.readBytesReference();
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(digests.length);
        for (int i = 0; i < digests.length; i++) {
            out.writeBytesReference(digests[i]);
        }
    }
}
