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
package io.crate.analyze;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.crate.analyze.where.PartitionResolver;
import io.crate.analyze.where.WhereClause;
import io.crate.exceptions.ColumnValidationException;
import io.crate.exceptions.SchemaUnknownException;
import io.crate.exceptions.TableUnknownException;
import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.metadata.*;
import io.crate.metadata.relation.TableRelation;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import io.crate.types.ObjectType;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public abstract class AbstractDataAnalysis extends Analysis {

    protected static final Predicate<ReferenceInfo> HAS_OBJECT_ARRAY_PARENT = new Predicate<ReferenceInfo>() {
        @Override
        public boolean apply(@Nullable ReferenceInfo input) {
            return input != null
                    && input.type().id() == ArrayType.ID
                    && ((ArrayType)input.type()).innerType().equals(DataTypes.OBJECT);
        }
    };

    protected final EvaluatingNormalizer normalizer;

    protected final ReferenceInfos referenceInfos;
    protected final Functions functions;
    protected final ReferenceResolver referenceResolver;
    private final PartitionResolver partitionResolver;
    protected TableInfo table;
    protected final List<String> ids = new ArrayList<>();
    protected final List<String> routingValues = new ArrayList<>();


    private final AllocationContext allocationContext;

    protected List<Symbol> outputSymbols = ImmutableList.of();

    private WhereClause whereClause = WhereClause.MATCH_ALL;
    protected RowGranularity rowGranularity;

    protected boolean onlyScalarsAllowed;
    protected boolean hasAggregates = false;
    protected boolean sysExpressionsAllowed = false;

    public AbstractDataAnalysis(ReferenceInfos referenceInfos,
                                Functions functions,
                                Analyzer.ParameterContext parameterContext,
                                ReferenceResolver referenceResolver) {
        super(parameterContext);
        this.partitionResolver = new PartitionResolver(referenceResolver, functions);
        this.allocationContext = new AllocationContext(referenceInfos);
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

    protected AllocationContext allocationContext() {
        return allocationContext;
    }

    @Deprecated
    @Override
    public void table(TableIdent tableIdent) {
        SchemaInfo schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        if (schemaInfo == null) {
            throw new SchemaUnknownException(tableIdent.schema());
        }
        TableInfo tableInfo = referenceInfos.getTableInfo(tableIdent);
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        }
        // if we have a system schema, queries require scalar functions, since those are not using lucene
        onlyScalarsAllowed = schemaInfo.systemSchema();
        sysExpressionsAllowed = schemaInfo.systemSchema();
        table = tableInfo;
        updateRowGranularity(table.rowGranularity());
    }

    @Deprecated
    public void editableTable(TableIdent tableIdent) throws TableUnknownException,
                                                            UnsupportedOperationException {
        SchemaInfo schemaInfo = referenceInfos.getSchemaInfo(tableIdent.schema());
        if (schemaInfo == null) {
            throw new SchemaUnknownException(tableIdent.schema());
        } else if (schemaInfo.systemSchema()) {
            throw new UnsupportedOperationException(
                    String.format("tables of schema '%s' are read only.", tableIdent.schema()));
        }
        TableInfo tableInfo = schemaInfo.getTableInfo(tableIdent.name());
        if (tableInfo == null) {
            throw new TableUnknownException(tableIdent.name());
        } else if (tableInfo.isAlias() && !tableInfo.isPartitioned()) {
            throw new UnsupportedOperationException(
                    String.format("aliases are read only cannot modify \"%s\"", tableIdent.name()));
        }
        table = tableInfo;
        allocationContext().currentRelation = new TableRelation(table, partitionResolver);
        updateRowGranularity(table.rowGranularity());
    }

    @Deprecated
    @Override
    public TableInfo table() {
        return this.table;
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     */
    @Deprecated
    protected boolean hasMatchingParent(ReferenceInfo info,
                              Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = table.getReferenceInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        return functions.getSafe(ident).info();
    }

    public Collection<Function> functions() {
        return allocationContext.allocatedFunctions.values();
    }

    @Deprecated
    public Function allocateFunction(FunctionInfo info, List<Symbol> arguments) {
        Function function = new Function(info, arguments);
        Function existing = allocationContext.allocatedFunctions.get(function);
        if (existing != null) {
            return existing;
        } else {
            if (info.type() == FunctionInfo.Type.AGGREGATE){
                hasAggregates = true;
                sysExpressionsAllowed = true;
            }
            allocationContext.allocatedFunctions.put(function, function);
        }
        return function;
    }

    /**
     * Indicates that the statement will not match, so that there is no need to execute it
     */
    @Deprecated
    public boolean noMatch() {
        return whereClause().noMatch();
    }

    public boolean hasNoResult() {
        return false;
    }

    @Deprecated
    public WhereClause whereClause(WhereClause whereClause) {
        this.whereClause = whereClause;
        return this.whereClause;
    }

    @Deprecated
    public WhereClause whereClause() {
        return whereClause;
    }

    /**
     * Updates the row granularity of this query if it is higher than the current row granularity.
     *
     * @param granularity the row granularity as seen by a reference
     */
    protected RowGranularity updateRowGranularity(RowGranularity granularity) {
        if (rowGranularity == null || rowGranularity.ordinal() < granularity.ordinal()) {
            rowGranularity = granularity;
        }
        return rowGranularity;
    }

    public RowGranularity rowGranularity() {
        return rowGranularity;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public void outputSymbols(List<Symbol> symbols) {
        this.outputSymbols = symbols;
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.planner.symbol.Reference}
     *
     * @param inputValue the value to normalize, might be anything from {@link io.crate.metadata.Scalar} to {@link io.crate.planner.symbol.Literal}
     * @param reference  the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws io.crate.exceptions.ColumnValidationException
     */
    public Literal normalizeInputForReference(Symbol inputValue, Reference reference) {
        Literal normalized;
        Symbol parameterOrLiteral = normalizer.process(inputValue, null);

        // 1. evaluate
        // guess type for dynamic column
        if (reference instanceof DynamicReference) {
            assert parameterOrLiteral instanceof Input;
            Object value = ((Input<?>)parameterOrLiteral).value();
            DataType guessed = DataTypes.guessType(value,
                    reference.info().objectType() == ReferenceInfo.ObjectType.IGNORED);
            ((DynamicReference) reference).valueType(guessed);
        }

        try {
            // resolve parameter to literal
            if (parameterOrLiteral.symbolType() == SymbolType.PARAMETER) {
                normalized = Literal.newLiteral(
                        reference.info().type(),
                        reference.info().type().value(((Parameter) parameterOrLiteral).value())
                );
            } else {
                try {
                    normalized = (Literal) parameterOrLiteral;
                    if (!normalized.valueType().equals(reference.info().type())) {
                        normalized = Literal.newLiteral(
                                reference.info().type(),
                                reference.info().type().value(normalized.value()));
                    }
                } catch (ClassCastException e) {
                    throw new ColumnValidationException(
                            reference.info().ident().columnIdent().name(),
                            String.format("Invalid value of type '%s'", inputValue.symbolType().name()));
                }
            }

            // 3. if reference is of type object - do special validation
            if (reference.info().type() == DataTypes.OBJECT) {
                Map<String, Object> value = (Map<String, Object>) normalized.value();
                if (value == null) {
                    return Literal.NULL;
                }
                normalized = Literal.newLiteral(normalizeObjectValue(value, reference.info()));
            } else if (isObjectArray(reference.info().type())) {
                Object[] value = (Object[]) normalized.value();
                if (value == null) {
                    return Literal.NULL;
                }
                normalized = Literal.newLiteral(
                        reference.info().type(),
                        normalizeObjectArrayValue(value, reference.info())
                );
            }
        } catch (ClassCastException | NumberFormatException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    SymbolFormatter.format(
                            "\"%s\" has a type that can't be implicitly cast to that of \"%s\" (" + reference.valueType().getName() + ")",
                            inputValue,
                            reference
                    ));
        }
        return normalized;
    }

    private boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType)type).innerType().id() == ObjectType.ID;
    }

    /**
     * normalize and validate the given value according to the given {@link io.crate.types.DataType}
     *
     * @param inputValue any {@link io.crate.planner.symbol.Symbol} that evaluates to a Literal or Parameter
     * @param dataType the type to convert this input to
     * @return a {@link io.crate.planner.symbol.Literal} of type <code>dataType</code>
     */
    public Literal normalizeInputForType(Symbol inputValue, DataType dataType) {
        Literal normalized;
        Symbol processed = normalizer.process(inputValue, null);
        if (processed instanceof Parameter) {
            normalized = Literal.newLiteral(dataType, dataType.value(((Parameter) processed).value()));
        } else {
            try {
                normalized = (Literal)processed;
                if (!normalized.valueType().equals(dataType)) {
                    normalized = Literal.newLiteral(dataType, dataType.value(normalized.value()));
                }
            } catch (ClassCastException | NumberFormatException e) {
                throw new IllegalArgumentException(
                        String.format(Locale.ENGLISH, "Invalid value of type '%s'", inputValue.symbolType().name()));
            }
        }
        return normalized;
    }


    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeObjectValue(Map<String, Object> value, ReferenceInfo info) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChild(info.ident().columnIdent(), entry.getKey());
            ReferenceInfo nestedInfo = table.getReferenceInfo(nestedIdent);
            if (nestedInfo == null) {
                if (info.objectType() == ReferenceInfo.ObjectType.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = table.dynamicReference(nestedIdent);
                DataType type = DataTypes.guessType(entry.getValue(), false);
                if (type == null) {
                    throw new ColumnValidationException(info.ident().columnIdent().fqn(), "Invalid value");
                }
                dynamicReference.valueType(type);
                nestedInfo = dynamicReference.info();
            } else {
                if (entry.getValue() == null) {
                    continue;
                }
            }
            if (nestedInfo.type() == DataTypes.OBJECT && entry.getValue() instanceof Map) {
                value.put(entry.getKey(), normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo));
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                value.put(entry.getKey(), normalizeObjectArrayValue((Object[])entry.getValue(), nestedInfo));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
        return value;
    }

    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            arrayItem = normalizeObjectValue((Map<String, Object>)arrayItem, arrayInfo);
        }
        return value;
    }

    private Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        try {
            if (info.type().equals(DataTypes.STRING) && primitiveValue instanceof String) {
                return primitiveValue;
            }
            return info.type().value(primitiveValue);
        } catch (Exception e) {
            throw new ColumnValidationException(info.ident().columnIdent().fqn(),
                    String.format("Invalid %s",
                            info.type().getName()
                    )
            );
        }
    }

    public void normalize() {
        normalizer.normalizeInplace(outputSymbols());
        if (whereClause().hasQuery()) {
            whereClause.normalize(normalizer);
            if (onlyScalarsAllowed && whereClause().hasQuery()){
                for (Function function : allocationContext.allocatedFunctions.keySet()) {
                    if (function.info().type() != FunctionInfo.Type.AGGREGATE
                            && !(functions.get(function.info().ident()) instanceof Scalar)){
                        throw new UnsupportedFeatureException(
                                "function not supported on system tables: " +  function.info().ident());
                    }
                }
            }
        }
    }

    /**
     * Compute an id and adds it, also add routing value
     */
    @Deprecated
    public void addIdAndRouting(List<BytesRef> primaryKeyValues, String clusteredByValue) {
        addIdAndRouting(false, primaryKeyValues, clusteredByValue);
    }

    @Deprecated
    protected void addIdAndRouting(Boolean create, List<BytesRef> primaryKeyValues, String clusteredByValue) {

        ColumnIdent clusteredBy = table().clusteredBy();
        Id id = new Id(table().primaryKey(), primaryKeyValues, clusteredBy == null ? null : clusteredBy, create);
        if (id.isValid()) {
            String idString = id.stringValue();
            ids.add(idString);
            if (clusteredByValue == null) {
                clusteredByValue = idString;
            }
        }
        if (clusteredByValue != null) {
            routingValues.add(clusteredByValue);
        }
    }

    @Deprecated
    public List<String> ids() {
        return ids;
    }

    @Deprecated
    public List<String> routingValues() {
        return routingValues;
    }

    public boolean hasSysExpressions() {
        return allocationContext.hasSysExpressions;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitDataAnalysis(this, context);
    }

    public PartitionResolver partitionResolver() {
        return partitionResolver;
    }
}
