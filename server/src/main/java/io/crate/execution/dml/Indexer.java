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
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
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
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.ParsedDocument;
import org.elasticsearch.index.mapper.SequenceIDFields;
import org.elasticsearch.index.mapper.Uid;
import org.jetbrains.annotations.Nullable;

import io.crate.analyze.SymbolEvaluator;
import io.crate.common.collections.Maps;
import io.crate.data.Input;
import io.crate.data.Row;
import io.crate.execution.engine.collect.CollectExpression;
import io.crate.execution.engine.collect.NestableCollectExpression;
import io.crate.expression.InputFactory;
import io.crate.expression.InputFactory.Context;
import io.crate.expression.reference.ReferenceResolver;
import io.crate.expression.symbol.DynamicReference;
import io.crate.expression.symbol.Literal;
import io.crate.expression.symbol.RefReplacer;
import io.crate.expression.symbol.Symbol;
import io.crate.expression.symbol.Symbols;
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
import io.crate.sql.tree.ColumnPolicy;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.ObjectType;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

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
 * <p>
 * This process is split into multiple steps:
 * <ol>
 * <li>Look for unknown columns via {@link #collectSchemaUpdates(IndexItem)}</li>
 * <li>Update cluster state with new columns (The user of the Indexer is
 * responsible for this)</li>
 * <li>Update reference information based on cluster state update</li>
 * <li>Create {@link ParsedDocument} via {@link #index(IndexItem)}</li>
 * </ol>
 *
 * Schema update and creation of ParsedDocument is split into two steps to be
 * able to use information from the persisted {@link Reference} from the cluster
 * state. (Mostly for the OID information)
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
    private final List<Synthetic> undeterministic = new ArrayList<>();
    private final BytesStreamOutput stream;
    private final boolean writeOids;
    private final Function<Reference, String> columnKeyProvider;
    private final Map<ColumnIdent, Reference> columnsByIdent = new HashMap<>();

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
                    return NestableCollectExpression.constant(ref.valueType().implicitCast(val));
                } else {
                    return NestableCollectExpression.constant(null);
                }
            }
            if (column.isRoot()) {
                if (targetColumns.contains(ref)) {
                    return NestableCollectExpression.constant(null);
                }
                Symbol defaultExpression = ref.defaultExpression();
                if (defaultExpression == null) {
                    if (ref instanceof GeneratedReference generated) {
                        return NestableCollectExpression.forFunction(
                            item -> fromGenerated(generated, item)
                        );
                    }
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
            if (rootIndex == -1) {
                return NestableCollectExpression.constant(null);
            }
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
                    } else if (ref instanceof GeneratedReference generated) {
                        return fromGenerated(generated, item);
                    }
                }
                return val;
            };
            return NestableCollectExpression.forFunction(getValue);
        }

        private Object fromGenerated(GeneratedReference generated, IndexItem item) {
            Symbol generatedExpression = RefReplacer.replaceRefs(generated.generatedExpression(), x -> {
                var collectExpression = getImplementation(x);
                collectExpression.setNextRow(item);
                return Literal.ofUnchecked(x.valueType(), collectExpression.value());
            });
            Input<?> accept = generatedExpression.accept(symbolEval, Row.EMPTY);
            return accept.value();
        }
    }

    /**
     * For DEFAULT expressions or GENERATED columns
     **/
    record Synthetic(Reference ref, Input<?> input, ValueIndexer<Object> indexer) {
    }

    interface ColumnConstraint {

        void verify(Object providedValue);
    }

    interface TableConstraint {

        void verify(Object[] values);
    }

    record NotNullTableConstraint(ColumnIdent column, Input<?> input) implements TableConstraint {

        @Override
        public void verify(Object[] values) {
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
        public void verify(Object[] values) {
            Object value = input.value();
            // SQL semantics: If a column is omitted from an INSERT/UPDATE statement,
            // CHECK constraints should not fail. Same for writing explicit `null` values.
            if (value instanceof Boolean bool) {
                if (!bool) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Failed CONSTRAINT %s CHECK (%s) for values: %s",
                        checkConstraint.name(),
                        checkConstraint.expressionStr(),
                        Arrays.toString(values)));
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
            checks.add(check1);
            checks.add(check2);
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
        Iterator<Reference> refsIterator = table.iterator();
        while (refsIterator.hasNext()) {
            Reference reference = refsIterator.next();
            columnsByIdent.put(reference.column(), reference);
        }
        this.symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        this.columns = targetColumns;
        this.synthetics = new HashMap<>();
        this.stream = new BytesStreamOutput();
        this.writeOids = table.versionCreated().onOrAfter(Version.V_5_5_0);
        if (writeOids) {
            columnKeyProvider = reference -> reference.oid() != COLUMN_OID_UNASSIGNED ? Long.toString(reference.oid()) : reference.column().leafName();
        } else {
            columnKeyProvider = reference -> reference.column().leafName();
        }
        PartitionName partitionName = table.isPartitioned()
            ? PartitionName.fromIndexOrTemplate(indexName)
            : null;
        InputFactory inputFactory = new InputFactory(nodeCtx);
        var referenceResolver = new RefResolver(symbolEval, partitionName, targetColumns, table);
        Context<CollectExpression<IndexItem, Object>> ctxForRefs = inputFactory.ctxForRefs(
            txnCtx,
            referenceResolver
        );
        Function<ColumnIdent, Reference> getRef = ident -> columnsByIdent.get(ident);
        this.valueIndexers = new ArrayList<>(targetColumns.size());
        int position = -1;

        for (var ref : targetColumns) {
            ValueIndexer<?> valueIndexer;
            if (ref instanceof DynamicReference dynamic) {
                if (table.columnPolicy() == ColumnPolicy.STRICT) {
                    throw new IllegalArgumentException(String.format(
                        Locale.ENGLISH,
                        "Cannot add column `%s` to table `%s` with column policy `strict`",
                        ref.column(),
                        table.ident()
                    ));
                }
                valueIndexer = new DynamicIndexer(ref.ident(), position, getFieldType, getRef);
                position--;
            } else {
                valueIndexer = ref.valueType().valueIndexer(
                    table.ident(),
                    ref,
                    getFieldType,
                    getRef
                );
            }
            this.valueIndexers.add(valueIndexer);
            addGeneratedToVerify(columnConstraints, table, ctxForRefs, ref);
        }
        this.tableConstraints = new ArrayList<>(table.checkConstraints().size());
        addNotNullConstraints(
            tableConstraints,
            columnConstraints,
            table,
            targetColumns,
            ctxForRefs
        );
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

            // To ensure default expressions of object children are evaluated it's necessary
            // to ensure the parent has values (or is present in the insert)
            // This is because of how the index routine works:
            // Processing objects is recursive, it skips null values -> it needs root synthetics as entry points
            for (ColumnIdent parent : column.parents()) {
                if (synthetics.containsKey(parent) || Symbols.containsColumn(targetColumns, parent)) {
                    continue;
                }
                Reference parentRef = table.getReference(parent);
                assert parentRef != null
                    : "Must be able to retrieve Reference for parent of a `defaultExpressionColumn`";

                int dimensions = ArrayType.dimensions(parentRef.valueType());
                if (dimensions > 0) {
                    break;
                }
                Input<?> input = HashMap::new;
                ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) parentRef.valueType().valueIndexer(
                    table.ident(),
                    parentRef,
                    getFieldType,
                    getRef
                );
                Synthetic synthetic = new Synthetic(parentRef, input, valueIndexer);
                this.synthetics.put(parent, synthetic);
            }

            Input<?> input = table.primaryKey().contains(column)
                ? ctxForRefs.add(ref)
                : ctxForRefs.add(ref.defaultExpression());
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) ref.valueType().valueIndexer(
                table.ident(),
                ref,
                getFieldType,
                getRef
            );
            Synthetic synthetic = new Synthetic(ref, input, valueIndexer);
            this.synthetics.put(column, synthetic);

            if (!Symbols.isDeterministic(ref.defaultExpression())) {
                undeterministic.add(synthetic);
            }
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
            Synthetic synthetic = new Synthetic(ref, input, valueIndexer);
            this.synthetics.put(ref.column(), synthetic);

            if (!Symbols.isDeterministic(ref.generatedExpression())) {
                undeterministic.add(synthetic);
            }
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
            if (fieldType.indexOptions() != IndexOptions.NONE) {
                indexColumns.add(new IndexColumn(ref.column(), fieldType, indexInputs));
            }
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

    /**
     * @param addedColumns has columns collected by {@link #collectSchemaUpdates(IndexItem) and with assigned OID0-s
     */
    public void updateTargets(List<Reference> addedColumns) {
        // Store by ident for fast replacement.
        Map<ColumnIdent, Reference> addedColumnsByIdent = new HashMap<>();
        for (Reference ref: addedColumns) {
            addedColumnsByIdent.put(ref.column(), ref);
            // Also add to columnsByIdent,so that getRef supplier doesn't get stale.
            columnsByIdent.put(ref.column(), ref);
        }

        for (int i = 0; i < columns.size(); i++) {
            Reference referenceWithOid = addedColumnsByIdent.get(columns.get(i).column());
            if (referenceWithOid != null) {
                columns.set(i, referenceWithOid);
            }
        }
    }

    private static void addNotNullConstraints(List<TableConstraint> tableConstraints,
                                              Map<ColumnIdent, ColumnConstraint> columnConstraints,
                                              DocTableInfo table,
                                              List<Reference> targetColumns,
                                              Context<CollectExpression<IndexItem, Object>> ctxForRefs) {
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
    }

    private static void addGeneratedToVerify(Map<ColumnIdent, ColumnConstraint> columnConstraints,
                                      DocTableInfo table,
                                      Context<?> ctxForRefs,
                                      Reference ref) {
        if (ref instanceof GeneratedReference generated
                && Symbols.isDeterministic(generated.generatedExpression())) {
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
                addGeneratedToVerify(columnConstraints, table, ctxForRefs, reference);
            }
        }
    }


    /**
     * Looks for new columns in the values of the given IndexItem and returns them.
     */
    public List<Reference> collectSchemaUpdates(IndexItem item) throws IOException {
        ArrayList<Reference> newColumns = new ArrayList<>();
        Consumer<? super Reference> onDynamicColumn = ref -> {
            ColumnIdent.validateColumnName(ref.column().name());
            ref.column().path().forEach(ColumnIdent::validateObjectKey);
            newColumns.add(ref);
        };

        for (var expression : expressions) {
            expression.setNextRow(item);
        }

        Object[] values = item.insertValues();
        for (int i = 0; i < values.length; i++) {
            Reference reference = columns.get(i);
            Object value = reference.valueType().valueForInsert(values[i]);
            // No granularity check since PARTITIONED BY columns cannot be added dynamically.
            if (value == null) {
                continue;
            }
            ValueIndexer<Object> valueIndexer = (ValueIndexer<Object>) valueIndexers.get(i);
            valueIndexer.collectSchemaUpdates(reference.valueType().sanitizeValue(value), onDynamicColumn, synthetics);
        }
        // Generated columns can result in new columns. For example: details object generated always as {\"a1\" = {\"b1\" = 'test'}},
        for (var entry : synthetics.entrySet()) {
            ColumnIdent column = entry.getKey();
            if (!column.isRoot()) {
                continue;
            }
            Synthetic synthetic = entry.getValue();
            ValueIndexer<Object> indexer = synthetic.indexer();
            Object value = synthetic.input().value();
            if (value == null) {
                continue;
            }
            indexer.collectSchemaUpdates(
                value,
                onDynamicColumn,
                synthetics
            );
        }
        return newColumns;
    }

    /**
     * Create a {@link ParsedDocument} from {@link IndexItem}
     *
     * This must be called after any new columns (found via
     * {@link #collectSchemaUpdates(IndexItem)}) have been added to the cluster
     * state.
     */
    @SuppressWarnings("unchecked")
    public ParsedDocument index(IndexItem item) throws IOException {
        assert item.insertValues().length <= valueIndexers.size()
            : "Number of values must be less than or equal the number of targetColumns/valueIndexers";

        Document doc = new Document();
        Consumer<? super IndexableField> addField = doc::add;
        for (var expression : expressions) {
            expression.setNextRow(item);
        }
        stream.reset();
        try (XContentBuilder xContentBuilder = XContentFactory.json(stream)) {
            xContentBuilder.startObject();
            Object[] values = item.insertValues();
            for (int i = 0; i < values.length; i++) {
                Reference reference = columns.get(i);
                    // It's possible to have target references without OID after doing a mapping update:
                    // Empty arrays, arrays with only null values and columns added dynamically into IGNORED object doesn't result in schema update.
                    assert assertExistingOid(reference) : "All target columns must have assigned OID on indexing.";

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
                xContentBuilder.field(columnKeyProvider.apply(reference));
                valueIndexer.indexValue(
                    reference.valueType().sanitizeValue(value),
                    xContentBuilder,
                    addField,
                    synthetics,
                    columnConstraints,
                    columnKeyProvider
                );
            }
            for (var entry : synthetics.entrySet()) {
                ColumnIdent column = entry.getKey();
                if (!column.isRoot()) {
                    continue;
                }
                Synthetic synthetic = entry.getValue();
                ValueIndexer<Object> indexer = synthetic.indexer();
                Object value = synthetic.input().value();
                if (value == null) {
                    continue;
                }
                xContentBuilder.field(columnKeyProvider.apply(synthetic.ref()));
                indexer.indexValue(
                    value,
                    xContentBuilder,
                    addField,
                    synthetics,
                    columnConstraints,
                    columnKeyProvider
                );
            }
            xContentBuilder.endObject();

            for (var indexColumn : indexColumns) {
                String fqn = indexColumn.name.fqn();
                for (var input : indexColumn.inputs) {
                    Object value = input.value();
                    if (value == null) {
                        continue;
                    }
                    if (value instanceof Iterable<?> it) {
                        for (Object val : it) {
                            if (val == null) {
                                continue;
                            }
                            Field field = new Field(fqn, val.toString(), indexColumn.fieldType);
                            doc.add(field);
                        }
                    } else {
                        Field field = new Field(fqn, value.toString(), indexColumn.fieldType);
                        doc.add(field);
                    }
                }
            }

            for (var constraint : tableConstraints) {
                constraint.verify(item.insertValues());
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
                null
            );
        }
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

    public static Consumer<IndexItem> createConstraintCheck(String indexName,
                                                            DocTableInfo table,
                                                            TransactionContext txnCtx,
                                                            NodeContext nodeCtx,
                                                            List<Reference> targetColumns) {
        var symbolEval = new SymbolEvaluator(txnCtx, nodeCtx, SubQueryResults.EMPTY);
        PartitionName partitionName = table.isPartitioned()
            ? PartitionName.fromIndexOrTemplate(indexName)
            : null;
        InputFactory inputFactory = new InputFactory(nodeCtx);
        var referenceResolver = new RefResolver(symbolEval, partitionName, targetColumns, table);
        Context<CollectExpression<IndexItem, Object>> ctxForRefs = inputFactory.ctxForRefs(
            txnCtx,
            referenceResolver
        );
        Map<ColumnIdent, ColumnConstraint> columnConstraints = new HashMap<>();
        for (var ref : targetColumns) {
            addGeneratedToVerify(columnConstraints, table, ctxForRefs, ref);
        }
        List<TableConstraint> tableConstraints = new ArrayList<>(table.checkConstraints().size());
        addNotNullConstraints(
            tableConstraints, columnConstraints, table, targetColumns, ctxForRefs);

        for (var constraint : table.checkConstraints()) {
            Symbol expression = constraint.expression();
            Input<?> input = ctxForRefs.add(expression);
            tableConstraints.add(new TableCheckConstraint(input, constraint));
        }
        return indexItem -> {
            for (var expression : ctxForRefs.expressions()) {
                expression.setNextRow(indexItem);
            }
            Object[] values = indexItem.insertValues();
            for (int i = 0; i < values.length; i++) {
                Reference reference = targetColumns.get(i);
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
            }
            for (var constraint : tableConstraints) {
                constraint.verify(values);
            }
        };
    }

    public boolean hasUndeterministicSynthetics() {
        return !undeterministic.isEmpty();
    }

    public List<Reference> columns() {
        return columns;
    }

    public Collection<Reference> insertColumns(List<Reference> columns) {
        if (undeterministic.isEmpty()) {
            return columns;
        }
        List<Reference> newColumns = new ArrayList<>(columns);
        for (var synthetic : undeterministic) {
            if (synthetic.ref.column().isRoot() && !newColumns.contains(synthetic.ref)) {
                newColumns.add(synthetic.ref);
            }
        }
        return newColumns;
    }

    @SuppressWarnings("unchecked")
    public Object[] addGeneratedValues(IndexItem item) {
        Object[] insertValues = item.insertValues();
        int numExtra = (int) undeterministic.stream()
            .filter(x -> x.ref().column().isRoot())
            .count();
        Object[] result = new Object[insertValues.length + numExtra];
        System.arraycopy(insertValues, 0, result, 0, insertValues.length);

        int i = 0;
        for (var synthetic : undeterministic) {
            ColumnIdent column = synthetic.ref.column();
            if (column.isRoot()) {
                result[insertValues.length + i] = synthetic.input.value();
                i++;
            } else {
                int valueIdx = Reference.indexOf(columns, column.getRoot());
                assert valueIdx > -1 : "synthetic column must exist in columns";

                ColumnIdent child = column.shiftRight();
                Object value = synthetic.input.value();
                Object object = insertValues[valueIdx];
                Maps.mergeInto(
                    (Map<String, Object>) object,
                    child.name(),
                    child.path(),
                    value
                );
            }
        }
        return result;
    }

    private boolean assertExistingOid(Reference ref) {
        if (writeOids && ref instanceof DynamicReference == false) {
            return ref.oid() != COLUMN_OID_UNASSIGNED;
        }
        return true;
    }
}
