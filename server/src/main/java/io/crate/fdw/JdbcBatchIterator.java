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

import static io.crate.types.ResultSetParser.getObject;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;

import org.apache.lucene.util.Accountable;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import io.crate.common.collections.Lists;
import io.crate.common.exceptions.Exceptions;
import io.crate.data.BatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.exceptions.ConversionException;
import io.crate.expression.scalar.cast.CastMode;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.SymbolType;
import io.crate.expression.symbol.SymbolVisitor;
import io.crate.expression.symbol.format.Style;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.sql.Identifiers;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class JdbcBatchIterator implements BatchIterator<Row> {

    private final String url;
    private final Properties properties;
    private final Row row;
    private final Object[] cells;
    private final List<Reference> columns;
    private final Symbol query;
    private final RelationName table;

    private Connection conn;
    private PreparedStatement statement;
    private ResultSet resultSet;
    private volatile Throwable killed = null;

    public JdbcBatchIterator(String url,
                             Properties properties,
                             List<Reference> columns,
                             Symbol query,
                             RelationName table) {
        this.url = url;
        this.properties = properties;
        this.columns = columns;
        this.query = query;
        this.table = table;
        this.cells = new Object[columns.size()];
        this.row = new RowN(cells);
    }

    static String generateStatement(RelationName table,
                                    List<Reference> columns,
                                    Symbol query,
                                    String quoteString) {
        final String qs = quoteString.isBlank() ? "" : quoteString;
        StringBuilder relationName = new StringBuilder();
        String schema = table.schema();
        if (schema != null) {
            relationName
                .append(qs)
                .append(schema)
                .append(qs)
                .append('.');
        }
        relationName
            .append(qs)
            .append(table.name())
            .append(qs);


        return String.format(
            Locale.ENGLISH,
            "SELECT %s FROM %s WHERE %s",
            String.join(", ", Lists.mapLazy(
                columns.stream().map(ref ->
                        new SimpleReference(
                            new ReferenceIdent(table, ColumnIdent.of(ref.column().name())),
                            ref.granularity(),
                            DataTypes.UNTYPED_OBJECT,
                            ref.position(),
                            ref.defaultExpression())).toList(),
                ref -> new QuotedReference(ref, qs).toString(Style.UNQUALIFIED))),
            relationName.toString(),
            RefReplacer.replaceRefs(
                query,
                ref -> new QuotedReference(ref, qs)).toString(Style.UNQUALIFIED)
        );
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
                    ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
                    Object object = getObject(resultSet, i, resultSetMetaData.getColumnTypeName(i + 1), ref);
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
        if (statement == null) {
            DatabaseMetaData metaData = conn.getMetaData();
            String sql = generateStatement(table, columns, query, metaData.getIdentifierQuoteString());
            statement = conn.prepareStatement(sql);
        }
        resultSet = statement.executeQuery();
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


    /**
     * Reference with custom toString implementation that quotes all identifiers with custom quoteString
     * Delegates everything else to another reference instance
     */
    static class QuotedReference implements Reference {

        private final Reference ref;
        private final String quoteString;

        private QuotedReference(Reference ref, String quoteString) {
            this.ref = ref;
            this.quoteString = quoteString;
        }

        @Override
        public String toString(Style style) {
            ColumnIdent column = ref.column();
            StringBuilder sb = new StringBuilder();
            if (style == Style.QUALIFIED) {
                RelationName tableIdent = ref.ident().tableIdent();
                String schema = tableIdent.schema();
                if (schema != null) {
                    sb.append(quoteString);
                    sb.append(schema);
                    sb.append(quoteString);
                    sb.append('.');
                }
                sb.append(quoteString);
                sb.append(tableIdent.name());
                sb.append(quoteString);
            }
            sb.append(quoteString);
            sb.append(Identifiers.escape(column.sqlFqn()));
            sb.append(quoteString);
            return sb.toString();
        }

        public long ramBytesUsed() {
            return ref.ramBytesUsed();
        }

        public Collection<Accountable> getChildResources() {
            return ref.getChildResources();
        }

        public void writeTo(StreamOutput out) throws IOException {
            ref.writeTo(out);
        }

        public ReferenceIdent ident() {
            return ref.ident();
        }

        public ColumnIdent column() {
            return ref.column();
        }

        public SymbolType symbolType() {
            return ref.symbolType();
        }

        public IndexType indexType() {
            return ref.indexType();
        }

        public <C, R> R accept(SymbolVisitor<C, R> visitor, C context) {
            return ref.accept(visitor, context);
        }

        public ColumnPolicy columnPolicy() {
            return ref.columnPolicy();
        }

        public boolean isNullable() {
            return ref.isNullable();
        }

        public DataType<?> valueType() {
            return ref.valueType();
        }

        public RowGranularity granularity() {
            return ref.granularity();
        }

        public Symbol cast(DataType<?> targetType, CastMode... modes) {
            return ref.cast(targetType, modes);
        }

        public int position() {
            return ref.position();
        }

        public long oid() {
            return ref.oid();
        }

        public boolean isDropped() {
            return ref.isDropped();
        }

        public boolean hasDocValues() {
            return ref.hasDocValues();
        }

        public @Nullable Symbol defaultExpression() {
            return ref.defaultExpression();
        }

        public boolean isGenerated() {
            return ref.isGenerated();
        }

        public Reference withReferenceIdent(ReferenceIdent referenceIdent) {
            return ref.withReferenceIdent(referenceIdent);
        }

        public Reference withOidAndPosition(LongSupplier acquireOid, IntSupplier acquirePosition) {
            return ref.withOidAndPosition(acquireOid, acquirePosition);
        }

        public Reference withDropped(boolean dropped) {
            return ref.withDropped(dropped);
        }

        public Reference withValueType(DataType<?> type) {
            return ref.withValueType(type);
        }

        public String storageIdent() {
            return ref.storageIdent();
        }

        public String storageIdentLeafName() {
            return ref.storageIdentLeafName();
        }

        public Map<String, Object> toMapping(int position) {
            return ref.toMapping(position);
        }
    }
}
