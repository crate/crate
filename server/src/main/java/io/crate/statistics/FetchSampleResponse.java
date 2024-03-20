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

package io.crate.statistics;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

import io.crate.metadata.Reference;

public final class FetchSampleResponse extends TransportResponse {

    private final Samples samples;

    FetchSampleResponse(Samples samples) {
        this.samples = samples;
    }

    public FetchSampleResponse(List<Reference> references, StreamInput in) throws IOException {
        this.samples = new Samples(references, in);
    }

    Samples samples() {
        return samples;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        samples.writeTo(out);
    }

    public static FetchSampleResponse merge(FetchSampleResponse s1, FetchSampleResponse s2) {
        return new FetchSampleResponse(Samples.merge(s1.samples(), s2.samples()));
    }
}
