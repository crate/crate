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

package io.crate.fdw;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

import org.jetbrains.annotations.NotNull;

import io.crate.common.collections.Lists;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ConversionException;
import io.crate.metadata.Reference;
import io.crate.metadata.RelationName;

public class JdbcBatchIterator implements BatchIterator<Row> {

    private final String url;
    private final Properties properties;
    private final String sql;
    private final Row row;
    private final Object[] cells;
    private final List<Reference> columns;

    private Connection conn;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private volatile Throwable killed = null;
    private final RelationName localName;

    public JdbcBatchIterator(String url, Properties properties, List<Reference> columns, RelationName remoteName, RelationName localName) {
        this.url = url;
        this.properties = properties;
        this.columns = columns;
        this.sql = String.format(
            Locale.ENGLISH,
            "SELECT %s FROM %s",
            String.join(", ", Lists.mapLazy(columns, ref -> ref.column().quotedOutputName())),
            remoteName.sqlFqn()
        );
        this.cells = new Object[columns.size()];
        this.row = new RowN(cells);
        this.localName = localName;
    }

    @Override
    public void kill(@NotNull Throwable throwable) {
        try {
            if (statement != null) {
                statement.close();
            }
            if (conn != null) {
                conn.close();
            }
        } catch (SQLException ignored) {
            throwable.addSuppressed(ignored);
        }
        killed = throwable;
    }

    @Override
    public Row currentElement() {
        return row;
    }

    @Override
    public void moveToStart() {
        raiseIfKilled();
        if (resultSet != null) {
            try {
                resultSet.beforeFirst();
            } catch (SQLException e) {
                resultSet = null;
            }
        }
    }

    @Override
    public boolean moveNext() {
        raiseIfKilled();
        if (resultSet == null) {
            return false;
        }
        try {
            if (resultSet.next()) {
                for (int i = 0; i < columns.size(); i ++) {
                    Reference ref = columns.get(i);
                    Object object = resultSet.getObject(i + 1);
                    try {
                        cells[i] = ref.valueType().implicitCast(object);
                    } catch (ClassCastException | IllegalArgumentException e) {
                        var conversionException = new ConversionException(object, ref.valueType());
                        conversionException.addSuppressed(e);
                        throw conversionException;
                    }
                }
                return true;
            }
            return false;
        } catch (SQLException e) {
            throw Exceptions.toRuntimeException(e);
        }
    }

    @Override
    public void close() {
        if (killed != null) {
            killed = BatchIterator.CLOSED;
        }
        if (conn != null) {
            try {
                conn.close();
            } catch (SQLException e) {
                throw Exceptions.toRuntimeException(e);
            }
        }
    }

    @Override
    public CompletionStage<?> loadNextBatch() throws Exception {
        if (conn == null) {
            conn = DriverManager.getConnection(url, properties);
        }
        try {
            if (statement == null) {
                statement = conn.prepareStatement(sql);
            }
            resultSet = statement.executeQuery();
        } catch (Exception e) {
            throw new IllegalStateException(
                String.format(
                    Locale.ENGLISH,
                    "The query on the foreign table, '%s' failed due to: %s",
                    localName.sqlFqn(),
                    e.getMessage()));
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public boolean allLoaded() {
        return resultSet != null;
    }

    @Override
    public boolean hasLazyResultSet() {
        return true;
    }

    private void raiseIfKilled() {
        if (killed != null) {
            Exceptions.rethrowUnchecked(killed);
        }
    }
}
