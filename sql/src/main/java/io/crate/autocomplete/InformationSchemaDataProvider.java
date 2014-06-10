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

package io.crate.autocomplete;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.crate.action.sql.SQLRequest;
import io.crate.action.sql.SQLResponse;
import io.crate.action.sql.TransportSQLAction;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;

/**
 * An Implementation of {@link io.crate.autocomplete.DataProvider} that uses the SQL Transport to retrieve
 * possible completions using SQLRequests - invoking statements that query the information schema.
 */
public class InformationSchemaDataProvider implements DataProvider {

    private final TransportSQLAction transportAction;

    @Inject
    public InformationSchemaDataProvider(TransportSQLAction transportAction) {
        this.transportAction = transportAction;
    }

    private ListenableFuture<List<String>> execute(String statement, Object[] args) {
        final SettableFuture<List<String>> result = SettableFuture.create();
        transportAction.execute(new SQLRequest(statement, args), new ActionListener<SQLResponse>() {
                @Override
                public void onResponse(SQLResponse response) {
                    List<String> rows = new ArrayList<>();
                    for (Object[] objects : response.rows()) {
                        rows.add((String) objects[0]);
                    }
                    result.set(rows);
                }

                @Override
                public void onFailure(Throwable e) {
                    result.setException(e);
                }
            });
        return result;
    }
    private ListenableFuture<List<String>> execute(String statement) {
        return execute(statement, new Object[0]);
    }

    @Override
    public ListenableFuture<List<String>> schemas(@Nullable String prefix) {
        if (prefix == null) {
            return execute("select distinct schema_name from information_schema.tables");
        }
        return execute("select distinct schema_name from information_schema.tables " +
            "where schema_name like ? ", new Object[] { prefix + "%" });
    }

    @Override
    public ListenableFuture<List<String>> tables(@Nullable String schema, @Nullable String prefix) {
        StringBuilder builder = new StringBuilder(
                "select distinct table_name from information_schema.tables where 1 = 1 ");
        List<Object> args = new ArrayList<>();
        if (schema != null) {
            builder.append("and schema_name = ? ");
            args.add(schema);
        }
        if (prefix != null) {
            builder.append("and table_name like ? ");
            args.add(prefix + "%");
        }
        return execute(builder.toString(), args.toArray(new Object[args.size()]));
    }

    @Override
    public ListenableFuture<List<String>> columns(@Nullable String schema, @Nullable String table, @Nullable String prefix) {
        StringBuilder builder = new StringBuilder(
                "select distinct column_name from information_schema.columns where 1 = 1 ");
        List<Object> args = new ArrayList<>();
        if (schema != null) {
            builder.append("and schema_name = ? ");
            args.add(schema);
        }

        if (table != null) {
            builder.append("and table_name = ? ");
            args.add(table);
        }

        if (prefix != null) {
            builder.append("and column_name like ? ");
            args.add(prefix + "%");
        }
        return execute(builder.toString(), args.toArray(new Object[args.size()]));
    }
}
