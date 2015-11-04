/*
 * Licensed to Crate.IO GmbH ("Crate") under one or more contributor
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

package io.crate.exceptions;

import java.util.Locale;
import java.util.UUID;

public class ContextMissingException extends UnhandledServerException {

    public enum ContextType {
        JOB_EXECUTION_CONTEXT("JobExecutionContext"),
        SUB_CONTEXT("ExecutionSubContext");

        private final String contextType;

        ContextType(String type) {
            contextType = type;
        }

        public String contextType() {
            return contextType;
        }
    }

    public ContextMissingException(ContextType type, UUID jobId) {
        super(String.format(Locale.ENGLISH, "%s for job %s not found", type.contextType(), jobId));
    }

    public ContextMissingException(ContextType type, UUID jobId, int contextId) {
        super(String.format(Locale.ENGLISH, "%s for job %s with id '%d' not found", type.contextType(), jobId, contextId));
    }

}
