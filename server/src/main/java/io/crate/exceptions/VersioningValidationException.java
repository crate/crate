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

import java.io.IOException;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;

import io.crate.rest.action.HttpErrorStatus;

public class VersioningValidationException extends ElasticsearchException implements UnscopedException {

    public static final String VERSION_COLUMN_USAGE_MSG = "\"_version\" column can only be used in the WHERE clause " +
                                                          "with equals comparisons and if there are also equals comparisons on primary key columns";

    public static final String SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG =
        "\"_seq_no\" and \"_primary_term\" columns can only be used together in the WHERE clause " +
        "with equals comparisons and if there are also equals comparisons on primary key columns";

    public static final String MIXED_VERSIONING_COLUMNS_USAGE_MSG =
        "\"_version\" column cannot be used in conjunction with the \"_seq_no\" and \"_primary_term\" columns. " +
        "Use one of the two versioning mechanisms, but not both at the same time.";

    private VersioningValidationException(String errorMessage) {
        super(errorMessage);
    }

    public static VersioningValidationException versionInvalidUsage() {
        return new VersioningValidationException(VERSION_COLUMN_USAGE_MSG);
    }

    public static VersioningValidationException seqNoAndPrimaryTermUsage() {
        return new VersioningValidationException(SEQ_NO_AND_PRIMARY_TERM_USAGE_MSG);
    }

    public static VersioningValidationException mixedVersioningMeachanismsUsage() {
        return new VersioningValidationException(MIXED_VERSIONING_COLUMNS_USAGE_MSG);
    }

    public VersioningValidationException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public HttpErrorStatus httpErrorStatus() {
        return HttpErrorStatus.STATEMENT_INVALID_OR_UNSUPPORTED_SYNTAX;
    }
}
