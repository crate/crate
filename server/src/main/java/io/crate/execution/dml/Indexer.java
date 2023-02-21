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


package io.crate.execution.dml;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

import javax.annotation.Nullable;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.CheckConstraint;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

/**
 * <p>
 *  Component to create a {@link ParsedDocument} from a {@link IndexItem}
 * </p>
 *
 * <p>Takes care of:</p>
 * <ul>
 *  <li>Validation of constraints (NOT NULL, CHECK)</li>
 *  <li>Adds DEFAULT values for _missing_ columns</li>
 *  <li>Adds GENERATED values</li>
 *  <li>Source generation</li>
 *  <li>Lucene {@link Document} and index field creation</li>
 * </ul>
 **/
public class Indexer {

    private final List<ValueIndexer<?>> valueIndexers;
    private final List<Reference> columns;
    private final SymbolEvaluator symbolEval;
    private final Map<ColumnIdent, Synthetic> synthetics;
    private final List<CollectExpression<IndexItem, Object>> expressions;
    private final Map<ColumnIdent, ColumnConstraint> columnConstraints = new HashMap<>();
    private final List<TableConstraint> tableConstraints;
    private final List<IndexColumn> indexColumns;
    private final List<Input<?>> returnValueInputs;

    record IndexColumn(ColumnIdent name, FieldType fieldType, List<Input<?>> inputs) {
    }

    static class RefResolver implements ReferenceResolver<CollectExpression<IndexItem, Object>> {

        private final PartitionName partitionName;
        private final List<Reference> targetColumns;
        private final DocTableInfo table;
        private final SymbolEvaluator symbolEval;

        private RefResolver(SymbolEvaluator symbolEval,
                            PartitionName partitionName,
                            List<Reference> targetColumns,
                            DocTableInfo table) {
            this.symbolEval = symbolEval;
            this.partitionName = partitionName;
            this.targetColumns = targetColumns;
            this.table = table;
        }

        @Override
        @SuppressWarnings("unchecked")
        public CollectExpression<IndexItem, Object> getImplementation(Reference ref) {

            // Here be dragons: A reference can refer to:
            //
            //  - To seqNo or primaryTerm of the IndexItem
            //
            //  - To a primary key that was already generated when resolving the routing.
            //    It must be part of `pkValues()` of the IndexItem
            //
            //  - value in the IndexItem insertValues.
            //    Example `INSERT INTO (x, y) VALUES (1, 2)
            //                          │             ▲
            //                          └─────────────┘
            //
            //  - Can be used in PARITITONED BY, in which case it's part of the partition name
            //
            //  - Can be missing from targetColumns / insertValues but have a DEFAULT clause
            //
            //  - Can be a child column, where the root is part of the targetColumns / insertValues

            ColumnIdent column = ref.column();
            if (column.equals(DocSysColumns.ID)) {
                return NestableCollectExpression.forFunction(IndexItem::id);
            } else if (column.equals(DocSysColumns.SEQ_NO)) {
                return NestableCollectExpression.forFunction(IndexItem::seqNo);
            } else if (column.equals(DocSysColumns.PRIMARY_TERM)) {
                return NestableCollectExpression.forFunction(IndexItem::primaryTerm);
            }
            int pkIndex = table.primaryKey().indexOf(column);
            if (pkIndex > -1) {
                return NestableCollectExpression.forFunction(item ->
                    ref.valueType().implicitCast(item.pkValues().get(pkIndex))
                );
            }
            int index = targetColumns.indexOf(ref);
            if (index > -1) {
                return NestableCollectExpression.forFunction(item -> item.insertValues()[index]);
            }
            if (ref.granularity() == RowGranularity.PARTITION) {
                int pIndex = table.partitionedByColumns().indexOf(ref);
                if (pIndex > -1) {
                    String val = partitionName.values().get(pIndex);
                    return NestableCollectExpression.constant(val);
                } else {
                    return NestableCollectExpression.constant(null);
                }
            }
            if (column.isTopLevel()) {
                if (targetColumns.contains(ref)) {
                    return NestableCollectExpression.constant(null);
                }
                Symbol defaultExpression = ref.defaultExpression();
                if (defaultExpression == null) {
                    return NestableCollectExpression.constant(null);
                }
                return NestableCollectExpression.constant(
                    defaultExpression.accept(symbolEval, Row.EMPTY).value()
                );
            }
            ColumnIdent root = column.getRoot();
            int rootIdx = -1;
            for (int i = 0; i < targetColumns.size(); i++) {
                if (targetColumns.get(i).column().equals(root)) {
                    rootIdx = i;
                    break;
                }
            }
            final int rootIndex = rootIdx;
            Function<IndexItem, Object> getValue = item -> {
                Object val = item.insertValues()[rootIndex];
                if (val instanceof Map<?, ?> m) {
                    List<String> path = column.path();
                    val = Maps.getByPath((Map<String, Object>) m, path);
                }
                if (val == null) {
                    Symbol defaultExpression = ref.defaultExpression();
                    if (defaultExpression != null) {
                        val = defaultExpression.accept(symbolEval, Row.EMPTY).value();
                    }
                }
                return val;
            };
            return NestableCollectExpression.forFunction(getValue);
        }
    }

    /**
     * For DEFAULT expressions or GENERATED columns
     **/
    record Synthetic(Input<?> input, ValueIndexer<Object> indexer) {
    }

    interface ColumnConstraint {

        void verify(Object providedValue);
    }

    interface TableConstraint {

        void verify();
    }

    record NotNullTableConstraint(ColumnIdent column, Input<?> input) implements TableConstraint {

        @Override
        public void verify() {
            Object value = input.value();
            if (value == null) {
                throw new IllegalArgumentException("\"" + column + "\" must not be null");
            }
        }
    }

    record TableCheckConstraint(
            Input<?> input,
            CheckConstraint<Symbol> checkConstraint) implements TableConstraint {

        @Override
        public void verify() {
            Object value = input.value();
            // SQL semantics: If a column is omitted from an INSERT/UPDATE statement,
            // CHECK constraints should not fail. Same for writing explicit `null` values.
            if (value instanceof Boolean bool) {
                if (!bool) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Failed CONSTRAINT %s CHECK (%s)",
                        checkConstraint.name(),
                        checkConstraint.expressionStr()));
                }
            }
        }
    }

    record MultiCheck(List<ColumnConstraint> checks) implements ColumnConstraint {

        @Override
        public void verify(Object providedValue) {
            for (var check : checks) {
                check.verify(providedValue);
            }
        }

        public static ColumnConstraint merge(ColumnConstraint check1, ColumnConstraint check2) {
            if (check1 instanceof MultiCheck multiCheck) {
                multiCheck.checks.add(check2);
                return check1;
            }
            ArrayList<ColumnConstraint> checks = new ArrayList<>(2);
            return new MultiCheck(checks);
        }
    }

    record NotNull(ColumnIdent column) implements ColumnConstraint {

        public void verify(Object providedValue) {
            if (providedValue == null) {
                throw new IllegalArgumentException("\"" + column + "\" must not be null");
            }
        }
    }

    record CheckGeneratedValue(Input<?> input, GeneratedReference ref) implements ColumnConstraint {

        @SuppressWarnings("unchecked")
        @Override
        public void verify(Object providedValue) {
            DataType<Object> valueType = (DataType<Object>) ref.valueType();
            Object generatedValue = input.value();
            int compare = Comparator
                .nullsFirst(valueType)
                .compare(
                    valueType.sanitizeValue(generatedValue),
                    valueType.sanitizeValue(providedValue)
                );
            if (compare != 0) {
                throw new IllegalArgumentException(
                    "Given value " + providedValue +
                    " for generated column " + ref.column() +
                    " does not match calculation " + ref.formattedGeneratedExpression() + " = " +
                    generatedValue
                );
            }
        }
    }

    @SuppressWarnings("unchecked")
    public Indexer(String indexName,
                   DocTableInfo table,
                   TransactionContext txnCtx,
                   NodeContext nodeCtx,
                   Function<ColumnIdent, FieldType> getFieldType,
                   List<Reference> targetColumns,
                   Symbol[] returnValues) {
        this.symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        this.columns = targetColumns;
        this.synthetics = new HashMap<>();
        PartitionName partitionName = table.isPartitioned()
            ? PartitionName.fromIndexOrTemplate(indexName)
            : null;
        InputFactory inputFactory = new InputFactory(nodeCtx);
        var referenceResolver = new RefResolver(symbolEval, partitionName, targetColumns, table);
        Context<CollectExpression<IndexItem, Object>> ctxForRefs = inputFactory.ctxForRefs(
            txnCtx,
            referenceResolver
        );
        Function<ColumnIdent, Reference> getRef = table::getReference;
        this.valueIndexers = new ArrayList<>(targetColumns.size());
        for (var ref : targetColumns) {
            this.valueIndexers.add(
                ref.valueType().valueIndexer(table.ident(), ref, getFieldType, getRef)
            );
            addToValidate(table, ctxForRefs, ref);
        }
        this.tableConstraints = new ArrayList<>(table.checkConstraints().size());
        for (var column : table.notNullColumns()) {
            Reference ref = table.getReference(column);
            assert ref != null : "Column in #notNullColumns must be available via table.getReference";
            if (targetColumns.contains(ref)) {
                columnConstraints.merge(ref.column(), new NotNull(column), MultiCheck::merge);
            } else if (ref instanceof GeneratedReference generated) {
                Input<?> input = ctxForRefs.add(generated.generatedExpression());
                tableConstraints.add(new NotNullTableConstraint(column, input));
            } else {
                Input<?> input = ctxForRefs.add(ref);
                tableConstraints.add(new NotNullTableConstraint(column, input));
            }
        }
        for (var constraint : table.checkConstraints()) {
            Symbol expression = constraint.expression();
            Input<?> input = ctxForRefs.add(expression);
            tableConstraints.add(new TableCheckConstraint(input, constraint));
        }
        for (var ref : table.defaultExpressionColumns()) {
            if (targetColumns.contains(ref) || ref.granularity() == RowGranularity.PARTITION) {
                continue;
            }
            ColumnIdent column = ref.column();
            Input<?> input = table.primaryKey().contains(column)
                ? ctxForRefs.add(ref)
                : ctxForRefs.add(ref.defaultExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getFieldType,
                getRef
            );
            this.synthetics.put(column, new Synthetic(input, valueIndexer));
        }
        for (var ref : table.generatedColumns()) {
            if (ref.granularity() == RowGranularity.PARTITION) {
                continue;
            }
            if (targetColumns.contains(ref)) {
                continue;
            }
            Input<?> input = ctxForRefs.add(ref.generatedExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getFieldType,
                getRef
            );
            this.synthetics.put(ref.column(), new Synthetic(input, valueIndexer));
        }
        this.indexColumns = new ArrayList<>(table.indexColumns().size());
        for (var ref : table.indexColumns()) {
            ArrayList<Input<?>> indexInputs = new ArrayList<>(ref.columns().size());
            FieldType fieldType = getFieldType.apply(ref.column());

            for (var sourceRef : ref.columns()) {
                Reference reference = table.getReference(sourceRef.column());
                assert reference.equals(sourceRef) : "refs must match";

                Input<?> input = ctxForRefs.add(sourceRef);
                indexInputs.add(input);
            }
            indexColumns.add(new IndexColumn(ref.column(), fieldType, indexInputs));
        }
        if (returnValues == null) {
            this.returnValueInputs = null;
        } else {
            this.returnValueInputs = new ArrayList<>(returnValues.length);
            for (Symbol returnValue : returnValues) {
                this.returnValueInputs.add(ctxForRefs.add(returnValue));
            }
        }
        this.expressions = ctxForRefs.expressions();
    }

    private void addToValidate(DocTableInfo table,
                               Context<?> ctxForRefs,
                               Reference ref) {
        if (ref instanceof GeneratedReference generated && ref.granularity() == RowGranularity.DOC) {
            Input<?> input = ctxForRefs.add(generated.generatedExpression());
            columnConstraints.put(ref.column(), new CheckGeneratedValue(input, generated));
        }
        if (ref.valueType() instanceof ObjectType objectType) {
            for (var entry : objectType.innerTypes().entrySet()) {
                String innerName = entry.getKey();
                ColumnIdent innerColumn = ref.column().getChild(innerName);
                Reference reference = table.getReference(innerColumn);
                if (reference == null) {
                    continue;
                }
                addToValidate(table, ctxForRefs, reference);
            }
        }
    }

    @SuppressWarnings("unchecked")
    public ParsedDocument index(IndexItem item) throws IOException {
        assert item.insertValues().length == valueIndexers.size()
            : "Number of values must match number of targetColumns/valueIndexers";

        Document doc = new Document();
        Consumer<? super IndexableField> addField = doc::add;
        ArrayList<Reference> newColumns = new ArrayList<>();
        Consumer<? super Reference> onDynamicColumn = newColumns::add;
        for (var expression : expressions) {
            expression.setNextRow(item);
        }
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        Object[] values = item.insertValues();
        for (int i = 0; i < values.length; i++) {
            Reference reference = columns.get(i);
            Object value = reference.valueType().valueForInsert(values[i]);
            ColumnConstraint check = columnConstraints.get(reference.column());
            if (check != null) {
                check.verify(value);
            }
            if (reference.granularity() == RowGranularity.PARTITION) {
                continue;
            }
            if (value == null) {
                continue;
            }
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(i);
            xContentBuilder.field(reference.column().leafName());
            valueIndexer.indexValue(
                value,
                xContentBuilder,
                addField,
                onDynamicColumn,
                synthetics,
                columnConstraints
            );
        }
        for (var entry : synthetics.entrySet()) {
            ColumnIdent column = entry.getKey();
            if (!column.isTopLevel()) {
                continue;
            }
            Synthetic synthetic = entry.getValue();
            ValueIndexer<Object> indexer = synthetic.indexer();
            Object value = synthetic.input().value();
            xContentBuilder.field(column.leafName());
            indexer.indexValue(
                value,
                xContentBuilder,
                addField,
                onDynamicColumn,
                synthetics,
                columnConstraints
            );
        }
        xContentBuilder.endObject();

        for (var indexColumn : indexColumns) {
            for (var input : indexColumn.inputs) {
                String value = (String) input.value();
                if (indexColumn.fieldType.indexOptions() != IndexOptions.NONE) {
                    Field field = new Field(indexColumn.name.fqn(), value, indexColumn.fieldType);
                    doc.add(field);
                }
            }
        }

        for (var constraint : tableConstraints) {
            constraint.verify();
        }

        NumericDocValuesField version = new NumericDocValuesField(DocSysColumns.Names.VERSION, -1L);
        doc.add(version);

        BytesReference source = BytesReference.bytes(xContentBuilder);
        BytesRef sourceRef = source.toBytesRef();
        doc.add(new StoredField("_source", sourceRef.bytes, sourceRef.offset, sourceRef.length));

        BytesRef idBytes = Uid.encodeId(item.id());
        doc.add(new Field(DocSysColumns.Names.ID, idBytes, IdFieldMapper.Defaults.FIELD_TYPE));

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        // Actual values are set via ParsedDocument.updateSeqID
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        return new ParsedDocument(
            version,
            seqID,
            item.id(),
            doc,
            source,
            null,
            newColumns
        );
    }

    @Nullable
    public Object[] returnValues(IndexItem item) {
        if (this.returnValueInputs == null) {
            return null;
        }
        expressions.forEach(x -> x.setNextRow(item));
        Object[] result = new Object[this.returnValueInputs.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = this.returnValueInputs.get(i).value();
        }
        return result;
    }
}
