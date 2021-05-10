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

package io.crate.exceptions;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import javax.annotation.Nullable;
import java.io.IOException;

public class JobKilledException extends ElasticsearchException implements UnscopedException {

    public static final String MESSAGE = "Job killed";

    public static JobKilledException of(@Nullable String reason) {
        return reason == null ? new JobKilledException() : new JobKilledException(reason);
    }

    public JobKilledException(final StreamInput in) throws IOException {
        super(in);
    }

    private JobKilledException(String reason) {
        super("Job killed. " + reason);
    }

    private JobKilledException() {
        super(MESSAGE);
    }

}
