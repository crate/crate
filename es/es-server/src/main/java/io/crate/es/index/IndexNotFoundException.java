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
package io.crate.es.index;

import io.crate.es.ResourceNotFoundException;
import io.crate.es.common.io.stream.StreamInput;

import java.io.IOException;

public final class IndexNotFoundException extends ResourceNotFoundException {
    /**
     * Construct with a custom message.
     */
    public IndexNotFoundException(String message, String index) {
        super(message);
        setIndex(index);
    }

    public IndexNotFoundException(String index) {
        this(index, (Throwable) null);
    }

    public IndexNotFoundException(String index, Throwable cause) {
        super("no such index", cause);
        setIndex(index);
    }

    public IndexNotFoundException(Index index) {
        this(index, null);
    }

    public IndexNotFoundException(Index index, Throwable cause) {
        super("no such index", cause);
        setIndex(index);
    }

    public IndexNotFoundException(StreamInput in) throws IOException {
        super(in);
    }
}
