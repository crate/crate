/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.action.sql;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.rest.RestStatus;

import java.util.List;

/**
 * This exception must be the only one which is thrown by our <code>TransportSQLAction</code>,
 * all internal exceptions must be transformed to this one.
 * All internal <code>CrateExceptions</code> exceptions must define a error code which is used here
 * to pass it outwards, also while creating this exception, an <code>RestStatus</code> must be
 * defined.
 * The stack trace given to the constructors, must be a string value in order to decouple
 * dependencies of nested exceptions, so a client doesn't need to know about all classes
 * related to these nested exceptions.
 */
public class SQLActionException extends ElasticsearchException {

    private static final String ERROR_CODE_KEY = "cr.ec";

    private final RestStatus status;

    public SQLActionException(String message, int errorCode, RestStatus status) {
        super(message);
        this.status = status;
        addHeader(ERROR_CODE_KEY, Integer.toString(errorCode));
    }

    /**
     * Construct a <code>SQLActionException</code> with the specified message, error code,
     * rest status code stack trace elements.
     *
     * @param message            the detailed message
     * @param errorCode          the crate error code
     * @param status             the rest status
     * @param stackTraceElements the stacktrace as array
     */
    public SQLActionException(String message, int errorCode, RestStatus status, StackTraceElement[] stackTraceElements) {
        this(message, errorCode, status);
        assert stackTraceElements != null : "stackTraceElements must not be null";
        setStackTrace(stackTraceElements);
    }

    /**
     * Return the rest status code defined on construction
     */
    public RestStatus status() {
        return status;
    }


    /**
     * Return the error code given defined on construction
     */
    public int errorCode() {
        List<String> errorCodeHeader = getHeader(ERROR_CODE_KEY);
        assert errorCodeHeader != null : "errorCodeHeader must not be null";
        return Integer.parseInt(errorCodeHeader.get(0));
    }

    @Override
    public String getDetailedMessage() {
        return status + " " + errorCode() + " " + super.getMessage();
    }

    @Override
    public String toString() {
        return "SQLActionException: " + getDetailedMessage();
    }
}
