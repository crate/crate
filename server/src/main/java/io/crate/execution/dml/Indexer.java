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

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.StoredField;
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
import io.crate.execution.engine.collect.ArrayCollectExpression;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.Symbol;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.GeneratedReference;
import io.crate.metadata.NodeContext;
import io.crate.metadata.Reference;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.TransactionContext;
import io.crate.metadata.doc.DocSysColumns;
import io.crate.metadata.doc.DocTableInfo;
import io.crate.planner.operators.SubQueryResults;
import io.crate.sql.tree.CheckConstraint;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

public class Indexer {

    private final List<ValueIndexer<?>> valueIndexers;
    private final List<Reference> columns;
    private final SymbolEvaluator symbolEval;
    private final Map<ColumnIdent, Synthetic> synthetics;
    private final List<CollectExpression<Object[], Object>> expressions;
    private final Map<ColumnIdent, Check> checks = new HashMap<>();
    private final List<Constraint> constraints;

    record Synthetic(Input<?> input, ValueIndexer<Object> indexer) {
    }

    interface Check {

        void verify(Object providedValue);
    }

    record Constraint(Input<?> input, CheckConstraint<Symbol> checkConstraint) {

        void verify() {
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

    record MultiCheck(List<Check> checks) implements Check {

        @Override
        public void verify(Object providedValue) {
            for (var check : checks) {
                check.verify(providedValue);
            }
        }

        public static Check merge(Check check1, Check check2) {
            if (check1 instanceof MultiCheck multiCheck) {
                multiCheck.checks.add(check2);
                return check1;
            }
            ArrayList<Check> checks = new ArrayList<>(2);
            return new MultiCheck(checks);
        }
    }

    record NotNull(ColumnIdent column) implements Check {

        public void verify(Object providedValue) {
            if (providedValue == null) {
                throw new IllegalArgumentException("\"" + column + "\" must not be null");
            }
        }
    }

    record CheckGeneratedValue(Input<?> input, GeneratedReference ref) implements Check {

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
    public Indexer(DocTableInfo table,
                   TransactionContext txnCtx,
                   NodeContext nodeCtx,
                   Function<ColumnIdent, FieldType> getFieldType,
                   List<Reference> targetColumns) {
        this.symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        this.columns = targetColumns;
        this.synthetics = new HashMap<>();
        InputFactory inputFactory = new InputFactory(nodeCtx);
        var referenceResolver = new ReferenceResolver<CollectExpression<Object[], Object>>() {

            @Override
            public CollectExpression<Object[], Object> getImplementation(Reference ref) {
                int index = targetColumns.indexOf(ref);
                if (index > -1) {
                    return new ArrayCollectExpression(index);
                }
                if (ref.column().isTopLevel()) {
                    Symbol defaultExpression = ref.defaultExpression();
                    if (defaultExpression == null || targetColumns.contains(ref)) {
                        return NestableCollectExpression.constant(null);
                    }
                    return NestableCollectExpression.constant(
                        defaultExpression.accept(symbolEval, Row.EMPTY).value()
                    );
                }
                ColumnIdent root = ref.column().getRoot();
                int rootIdx = -1;
                for (int i = 0; i < targetColumns.size(); i++) {
                    if (targetColumns.get(i).column().equals(root)) {
                        rootIdx = i;
                        break;
                    }
                }
                final int rootIndex = rootIdx;
                Function<Object[], Object> getValue = array -> {
                    Object val = array[rootIndex];
                    if (val instanceof Map<?, ?> m) {
                        List<String> path = ref.column().path();
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
        };
        Context<CollectExpression<Object[], Object>> ctxForRefs = inputFactory.ctxForRefs(
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
        for (var column : table.notNullColumns()) {
            Reference ref = table.getReference(column);
            assert ref != null : "Column in #notNullColumns must be available via table.getReference";
            checks.merge(ref.column(), new NotNull(column), MultiCheck::merge);
        }
        this.constraints = new ArrayList<>(table.checkConstraints().size());
        for (var constraint : table.checkConstraints()) {
            Symbol expression = constraint.expression();
            Input<?> input = ctxForRefs.add(expression);
            constraints.add(new Constraint(input, constraint));
        }
        for (var ref : table.defaultExpressionColumns()) {
            if (targetColumns.contains(ref) || ref.granularity() == RowGranularity.PARTITION) {
                continue;
            }
            Input<?> input = ctxForRefs.add(ref.defaultExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getFieldType,
                getRef
            );
            this.synthetics.put(ref.column(), new Synthetic(input, valueIndexer));
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
        this.expressions = ctxForRefs.expressions();
    }

    private void addToValidate(DocTableInfo table,
                               Context<CollectExpression<Object[], Object>> ctxForRefs,
                               Reference ref) {
        if (ref instanceof GeneratedReference generated && ref.granularity() == RowGranularity.DOC) {
            Input<?> input = ctxForRefs.add(generated.generatedExpression());
            checks.put(ref.column(), new CheckGeneratedValue(input, generated));
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
    public ParsedDocument index(String id, Object ... values) throws IOException {
        assert values.length == valueIndexers.size()
            : "Number of values must match number of targetColumns/valueIndexers";

        Document doc = new Document();
        Consumer<? super IndexableField> addField = doc::add;
        ArrayList<Reference> newColumns = new ArrayList<>();
        Consumer<? super Reference> onDynamicColumn = newColumns::add;
        // TODO: re-use stream?
        for (var expression : expressions) {
            expression.setNextRow(values);
        }
        XContentBuilder xContentBuilder = XContentFactory.jsonBuilder();
        xContentBuilder.startObject();
        for (int i = 0; i < values.length; i++) {
            Reference reference = columns.get(i);
            Symbol defaultExpression = reference.defaultExpression();
            Object value = reference.valueType().valueForInsert(values[i]);
            if (value == null && defaultExpression != null && !columns.contains(reference)) {
                value = defaultExpression.accept(symbolEval, Row.EMPTY).value();
            }
            Check check = checks.get(reference.column());
            if (check != null) {
                check.verify(value);
            }
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(i);
            xContentBuilder.field(reference.column().leafName());
            valueIndexer.indexValue(
                value,
                xContentBuilder,
                addField,
                onDynamicColumn,
                synthetics,
                checks
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
                checks
            );
        }
        xContentBuilder.endObject();

        for (var constraint : constraints) {
            constraint.verify();
        }

        NumericDocValuesField version = new NumericDocValuesField(DocSysColumns.Names.VERSION, -1L);
        doc.add(version);

        BytesReference source = BytesReference.bytes(xContentBuilder);
        BytesRef sourceRef = source.toBytesRef();
        doc.add(new StoredField("_source", sourceRef.bytes, sourceRef.offset, sourceRef.length));

        BytesRef idBytes = Uid.encodeId(id);
        doc.add(new Field(DocSysColumns.Names.ID, idBytes, IdFieldMapper.Defaults.FIELD_TYPE));

        SequenceIDFields seqID = SequenceIDFields.emptySeqID();
        doc.add(seqID.seqNo);
        doc.add(seqID.seqNoDocValue);
        doc.add(seqID.primaryTerm);
        return new ParsedDocument(
            version,
            seqID,
            id,
            doc,
            source,
            null,
            newColumns
        );
    }

    /**
     * TODO: remove once all indexers are implemented
     * @deprecated
     */
    public boolean isSupported() {
        return true && valueIndexers.stream().noneMatch(x -> x == null)
            && columns.stream().noneMatch(x -> x.valueType().id() == ObjectType.ID)
            && columns.stream().noneMatch(x -> x.column().equals(DocSysColumns.RAW))
            && columns.stream().noneMatch(x -> x.valueType() instanceof ArrayType);
    }
}
