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

package io.crate.action.sql;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 * SQL requests can be made either as bulk operation or as single operation.
 *
 * The TransportActions TransportSQLAction TransportSQLBulkAction
 * are responsible to execute them.
 *
 * In order to use the actions either send a
 * {@link io.crate.action.sql.SQLRequest} or
 * {@link SQLBulkRequest}
 *
 * to them using the SQLAction or SQLBulkAction
 *
 *
 * this abstract base class provides the shared components
 * {@link #stmt()} and {@link #includeTypesOnResponse()}
 * which both concrete classes use.
 *
 * (not using links for TransportSQLAction as they're not included for the client and would case an error under oraclejdk8)
 */
public abstract class SQLBaseRequest extends ActionRequest<SQLBaseRequest> {

    private static final String SCHEMA_HEADER_KEY = "_s";
    public static final String FLAGS_HEADER_KEY = "flags";

    // Bit flags for request header
    public static final int HEADER_FLAG_OFF = 0;
    public static final int HEADER_FLAG_ALLOW_QUOTED_SUBSCRIPT = 1;

    private String stmt;

    public SQLBaseRequest() {}

    public SQLBaseRequest(String stmt) {
        this.stmt = stmt;
    }

    /**
     * SQL statement as string.
     */
    public String stmt() {
        return stmt;
    }

    public SQLBaseRequest stmt(String stmt){
        this.stmt = stmt;
        return this;
    }

    /**
     * @deprecated types are now always included, setting this has no effect.
     */
    @Deprecated
    public void includeTypesOnResponse(boolean includeTypesOnResponse) {
    }

    /**
     * See also {@link #includeTypesOnResponse(boolean)}
     * @deprecated types are now always included
     * @return true
     */
    @Deprecated
    public boolean includeTypesOnResponse() {
        return true;
    }


    public void setDefaultSchema(String schemaName) {
        if (schemaName == null) {
            if (hasHeader(SCHEMA_HEADER_KEY)) {
                // can't remove header but want to reset schemaName...
                putHeader(SCHEMA_HEADER_KEY, null);
            }
            return; // don't set schemaName if null to avoid overhead
        }
        putHeader(SCHEMA_HEADER_KEY, schemaName);
    }

    public int getRequestFlags() {
        try {
            Integer flags = getHeader(FLAGS_HEADER_KEY);
            if (flags == null) {
                return HEADER_FLAG_OFF;
            }
            return flags;
        } catch (ClassCastException ex) {
            return HEADER_FLAG_OFF;
        }
    }

    @Nullable
    public String getDefaultSchema() {
        return getHeader(SCHEMA_HEADER_KEY);
    }

    @Override
    public ActionRequestValidationException validate() {
        if (stmt == null) {
            ActionRequestValidationException e =  new ActionRequestValidationException();
            e.addValidationError("Attribute 'stmt' must not be null");
            return e;
        }
        return null;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        stmt = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(stmt);
    }
}
