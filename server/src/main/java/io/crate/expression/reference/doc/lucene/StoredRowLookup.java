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

package io.crate.expression.reference.doc.lucene;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.compress.CompressorFactory;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;

import io.crate.common.collections.Maps;
import io.crate.execution.engine.fetch.ReaderContext;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.DocReferences;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.GeoPointType;
import io.crate.types.ObjectType;

public abstract class StoredRowLookup implements StoredRow {

    public static final Version PARTIAL_STORED_SOURCE_VERSION = Version.V_5_9_0;

    protected final PartitionValueInjector partitionValueInjector;
    protected final DocTableInfo table;

    protected int doc;
    protected ReaderContext readerContext;
    protected boolean docVisited = false;
    protected Map<String, Object> parsedSource = null;

    public static StoredRowLookup create(DocTableInfo table, String indexName) {
        return create(table, indexName, List.of(), false);
    }

    public static StoredRowLookup create(DocTableInfo table, String indexName, List<Symbol> columns, boolean fromTranslog) {
        if (table.versionCreated().before(PARTIAL_STORED_SOURCE_VERSION) || fromTranslog) {
            return new FullStoredRowLookup(table, indexName, columns);
        }
        return new ColumnAndStoredRowLookup(table, indexName, columns);
    }

    private StoredRowLookup(DocTableInfo table, String indexName) {
        this.partitionValueInjector = PartitionValueInjector.create(indexName, table.partitionedByColumns());
        this.table = table;
    }

    public final StoredRow getStoredRow(ReaderContext context, int doc) {
        if (this.doc == doc
                && this.readerContext != null
                && this.readerContext.reader() == context.reader()) {
            // Don't invalidate source
            return this;
        }
        this.doc = doc;
        this.readerContext = context;
        try {
            moveToDoc();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return this;
    }

    protected abstract void moveToDoc() throws IOException;

    protected abstract void registerRef(Reference ref);

    public final void register(List<Symbol> symbols) {
        if (symbols != null && Symbols.hasColumn(symbols, DocSysColumns.DOC) == false) {
            Consumer<Reference> register = ref -> {
                if (ref.column().isSystemColumn() == false && ref.granularity() == RowGranularity.DOC) {
                    registerRef(DocReferences.toDocLookup(ref));
                }
            };
            for (Symbol symbol : symbols) {
                symbol.visit(Reference.class, register);
            }
        } else {
            registerAll();
        }
    }

    public void registerAll() {
        for (Reference ref : table.columns()) {
            if (ref.column().isSystemColumn() == false & ref.granularity() == RowGranularity.DOC) {
                registerRef(DocReferences.toDocLookup(ref));
            }
        }
    }

    private static class FullStoredRowLookup extends StoredRowLookup {

        private final SourceFieldVisitor fieldsVisitor = new SourceFieldVisitor();
        private final SourceParser sourceParser;

        public FullStoredRowLookup(DocTableInfo table, String indexName, List<Symbol> columns) {
            super(table, indexName);
            this.sourceParser = new SourceParser(table.droppedColumns(), table.lookupNameBySourceKey());
            register(columns);
        }

        @Override
        protected void registerRef(Reference ref) {
            sourceParser.register(ref.column(), ref.valueType());
        }

        @Override
        protected void moveToDoc() {
            fieldsVisitor.reset();
            this.docVisited = false;
            this.parsedSource = null;
        }

        // On build source map, load via StoredFieldsVisitor and pass to SourceParser
        @Override
        public Map<String, Object> asMap() {
            if (docVisited == false) {
                parsedSource = partitionValueInjector.injectValues(sourceParser.parse(loadStoredFields()));
            }
            return parsedSource;
        }

        @Override
        public String asString() {
            if (docVisited == false) {
                loadStoredFields();
            }
            try {
                return CompressorFactory.uncompressIfNeeded(fieldsVisitor.source()).utf8ToString();
            } catch (IOException e) {
                throw new UncheckedIOException("Failed to decompress source", e);
            }
        }

        private BytesReference loadStoredFields() {
            try {
                readerContext.visitDocument(doc, fieldsVisitor);
                docVisited = true;
                return fieldsVisitor.source();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private static class ColumnAndStoredRowLookup extends StoredRowLookup {

        private record ColumnExpression(LuceneCollectorExpression<?> expression, ColumnIdent ident) {}

        private final List<ColumnExpression> expressions = new ArrayList<>();
        private final ColumnFieldVisitor fieldsVisitor = new ColumnFieldVisitor();

        private ColumnAndStoredRowLookup(DocTableInfo table, String indexName, List<Symbol> columns) {
            super(table, indexName);
            register(columns);
        }

        @Override
        protected void registerRef(Reference ref) {
            registerRef(ref, false);
        }

        private void registerRef(Reference ref, boolean fromParents) {
            if (fromParents == false) {
                // if we are inside an array, or an object type with ignored fields, then we need to load
                // the relevant parent from stored fields
                var storedParent = table.findParentReferenceMatching(ref, r ->
                    r.valueType() instanceof ObjectType && r.columnPolicy() == ColumnPolicy.IGNORED ||
                        r.valueType() instanceof ArrayType<?>);
                if (storedParent != null) {
                    this.fieldsVisitor.registerRef(storedParent);
                }
            }
            if (ref.hasColumn(DocSysColumns.DOC) || ref.hasColumn(DocSysColumns.RAW)) {
                // top-level _doc - we register all table columns
                registerAll();
            } else if (ref.valueType() instanceof GeoPointType) {
                // geopoint always retrieved from stored fields
                this.fieldsVisitor.registerRef(ref);
            } else if (ref.valueType() instanceof ArrayType<?>) {
                // arrays always retrieved from stored fields
                this.fieldsVisitor.registerRef(ref);
            } else if (ref.valueType() instanceof ObjectType) {
                this.fieldsVisitor.registerRef(ref);
                for (var leaf : table.getChildReferences(ref)) {
                    registerRef(leaf, true);
                }
            } else {
                LuceneCollectorExpression<?> expr = LuceneReferenceResolver.typeSpecializedExpression(ref);
                if (expr instanceof DocCollectorExpression<?>) {
                    this.fieldsVisitor.registerRef(ref);
                } else {
                    var column = ref.toColumn();
                    if (column.isRoot() == false && column.name().equals(DocSysColumns.Names.DOC)) {
                        column = column.shiftRight();
                    }
                    expressions.add(new ColumnExpression(expr, column));
                }
            }
        }

        @Override
        protected void moveToDoc() throws IOException {
            fieldsVisitor.reset();
            this.docVisited = false;
            this.parsedSource = null;
            for (var expr : expressions) {
                // TODO cache reader context and ensure docs are in-order
                expr.expression.setNextReader(readerContext);
                expr.expression.setNextDocId(doc);
            }
        }

        @Override
        public Map<String, Object> asMap() {
            if (docVisited == false) {
                try {
                    parsedSource = buildDocMap();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            return parsedSource;
        }

        @Override
        public String asString() {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            try (XContentBuilder builder = XContentFactory.json(output)) {
                builder.map(asMap());
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return output.toString();
        }

        private Map<String, Object> buildDocMap() throws IOException {
            Map<String, Object> docMap = storedMap();
            for (var expr : expressions) {
                var value = expr.expression.value();
                if (value != null) {
                    Maps.mergeInto(docMap, expr.ident.name(), expr.ident.path(), value);
                }
            }
            docMap = partitionValueInjector.injectValues(docMap);
            docVisited = true;
            return docMap;
        }

        private Map<String, Object> storedMap() throws IOException {
            if (fieldsVisitor.shouldLoadStoredFields()) {
                readerContext.visitDocument(doc, fieldsVisitor);
                return fieldsVisitor.getDocMap();
            } else {
                return new HashMap<>();
            }
        }

    }
}
