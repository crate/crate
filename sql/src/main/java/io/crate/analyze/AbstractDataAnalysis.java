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

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import io.crate.Id;
import io.crate.exceptions.*;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.operation.Input;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.ArrayType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

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
    private boolean onlyScalarsAllowed;

    protected final ReferenceInfos referenceInfos;
    private final Functions functions;
    protected SchemaInfo schema;
    protected TableInfo table;
    protected final List<String> ids = new ArrayList<>();
    protected final List<String> routingValues = new ArrayList<>();

    private Map<Function, Function> functionSymbols = new HashMap<>();

    protected Map<ReferenceIdent, Reference> referenceSymbols = new HashMap<>();

    protected List<Symbol> outputSymbols = ImmutableList.of();

    protected WhereClause whereClause = WhereClause.MATCH_ALL;
    protected RowGranularity rowGranularity;
    protected boolean hasAggregates = false;
    protected boolean hasSysExpressions = false;
    protected boolean sysExpressionsAllowed = false;

    public AbstractDataAnalysis(ReferenceInfos referenceInfos, Functions functions,
                                Object[] parameters,
                                ReferenceResolver referenceResolver) {
        super(parameters);
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

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
        schema = schemaInfo;
        onlyScalarsAllowed = schemaInfo.systemSchema();
        sysExpressionsAllowed = schemaInfo.systemSchema();
        table = tableInfo;
        updateRowGranularity(table.rowGranularity());
    }

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
        schema = schemaInfo;
        table = tableInfo;
        updateRowGranularity(table.rowGranularity());
    }

    @Override
    public TableInfo table() {
        return this.table;
    }

    @Override
    public SchemaInfo schema() {
        return this.schema;
    }

    private Reference allocateReference(ReferenceIdent ident, boolean unique) {
        if (ident.tableIdent().schema() != null
                && ident.tableIdent().schema().equals(SysSchemaInfo.NAME)) {
            hasSysExpressions = true;
        }
        Reference reference = referenceSymbols.get(ident);
        if (reference == null) {
            ReferenceInfo info = getReferenceInfo(ident);
            if (info == null) {
                reference = table.getDynamic(ident.columnIdent());
                if (reference == null) {
                    throw new ColumnUnknownException(ident.tableIdent().name(), ident.columnIdent().fqn());
                }
                info = reference.info();
            } else {
                reference = new Reference(info);
            }
            referenceSymbols.put(info.ident(), reference);
        } else if (unique) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", ident.columnIdent().fqn()));
        }
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    /**
     * add a reference for the given ident, get from map-cache if already
     *
     * @param ident
     * @return
     */
    public Reference allocateReference(ReferenceIdent ident) {
        return allocateReference(ident, false);
    }

    /**
     * add a new reference for the given ident
     * and throw an error if this ident has already been added
     */
    public Reference allocateUniqueReference(ReferenceIdent ident) {
        return allocateReference(ident, true);
    }

    @Nullable
    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceInfos.getReferenceInfo(ident);
        if (info != null &&
                !info.ident().isColumn() &&
                hasMatchingParent(info, HAS_OBJECT_ARRAY_PARENT)) {
            if (DataTypes.isCollectionType(info.type())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException(
                        "cannot query for arrays inside object arrays explicitly");
            }

            // for child fields of object arrays
            // return references of primitive types as arrays
            info = ReferenceInfo.builder()
                    .ident(info.ident())
                    .objectType(info.objectType())
                    .granularity(info.granularity())
                    .type(new ArrayType(info.type()))
                    .build();
        }

        return info;
    }

    /**
     * return true if the given {@linkplain com.google.common.base.Predicate}
     * returns true for a parent column of this one.
     * returns false if info has no parent column.
     * @param info
     * @param parentMatchPredicate
     * @return
     */
    protected boolean hasMatchingParent(ReferenceInfo info,
                              Predicate<ReferenceInfo> parentMatchPredicate) {
        ColumnIdent parent = info.ident().columnIdent().getParent();
        while (parent != null) {
            ReferenceInfo parentInfo = table.getColumnInfo(parent);
            if (parentMatchPredicate.apply(parentInfo)) {
                return true;
            }
            parent = parent.getParent();
        }
        return false;
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException(
                    String.format("unknown function: %s(%s)", ident.name(),
                            Joiner.on(", ").join(ident.argumentTypes())));
        }
        return implementation.info();
    }

    public FunctionImplementation getFunctionImplementation(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException(
                    String.format("unknown function: %s(%s)", ident.name(),
                            Joiner.on(", ").join(ident.argumentTypes())));
        }
        return implementation;
    }

    public Collection<Reference> references() {
        return referenceSymbols.values();
    }

    public Collection<Function> functions() {
        return functionSymbols.values();
    }

    public Function allocateFunction(FunctionInfo info, List<Symbol> arguments) {
        Function function = new Function(info, arguments);
        Function existing = functionSymbols.get(function);
        if (existing != null) {
            return existing;
        } else {
            if (info.isAggregate()){
                hasAggregates = true;
                sysExpressionsAllowed = true;
            }
            functionSymbols.put(function, function);
        }
        return function;
    }

    /**
     * Indicates that the statement will not match, so that there is no need to execute it
     */
    public boolean noMatch() {
        return whereClause().noMatch();
    }

    public boolean hasNoResult() {
        return false;
    }

    public WhereClause whereClause(WhereClause whereClause) {
        this.whereClause = whereClause.normalize(normalizer);
        return this.whereClause;
    }

    public WhereClause whereClause(Symbol whereClause) {
        this.whereClause = new WhereClause(normalizer.process(whereClause, null));
        return this.whereClause;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    /**
     * Updates the row granularity of this query if it is higher than the current row granularity.
     *
     * @param granularity the row granularity as seen by a reference
     * @return
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
                Map<String, Object> value = (Map<String, Object>) (normalized).value();
                if (value == null) {
                    return Literal.NULL;
                }
                normalized = Literal.newLiteral(normalizeObjectValue(value, reference.info()));
            }
        } catch (ClassCastException | NumberFormatException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    SymbolFormatter.format("\"%s\" has a type that can't be implicitly cast to that of \"%s\"",
                            inputValue,
                            reference
                    ));
        }
        return normalized;
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
            ReferenceInfo nestedInfo = table.getColumnInfo(nestedIdent);
            if (nestedInfo == null) {
                if (info.objectType() == ReferenceInfo.ObjectType.IGNORED) {
                    continue;
                }
                DynamicReference dynamicReference = table.getDynamic(nestedIdent);
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
            if (info.type() == DataTypes.OBJECT && entry.getValue() instanceof Map) {
                value.put(entry.getKey(), normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
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
                for (Function function : functionSymbols.keySet()) {
                    if (!function.info().isAggregate() && !(functions.get(function.info().ident()) instanceof Scalar)){
                        throw new UnsupportedFeatureException(
                                "function not supported on system tables: " +  function.info().ident());
                    }
                }
            }
        }
    }

    /**
     * get a reference from a given {@link io.crate.sql.tree.QualifiedName}
     */
    protected ReferenceIdent getReference(QualifiedName name) {
        return getReference(name, ImmutableList.<String>of());
    }

    protected ReferenceIdent getReference(QualifiedName name, List<String> path) {
        ReferenceIdent ident;
        List<String> qNameParts = name.getParts();
        switch (qNameParts.size()) {
            case 1:
                ident = new ReferenceIdent(table.ident(), qNameParts.get(0), path);
                break;
            case 2:
                if (tableAlias() != null) {
                    if (!tableAlias().equals(qNameParts.get(0))) {
                        throw new UnsupportedOperationException("table for reference not found in FROM: " + name);
                    }
                } else {
                    if (!table().ident().name().equals(qNameParts.get(0))) {
                        throw new UnsupportedOperationException("table for reference not found in FROM: " + name);
                    }
                }
                ident = new ReferenceIdent(table.ident(), qNameParts.get(1), path);
                break;
            case 3:
                if (tableAlias() != null && !"sys".equals(qNameParts.get(0).toLowerCase())) {
                    throw new UnsupportedOperationException("table for reference not found in FROM: " + name);
                }
                TableInfo otherTable = referenceInfos.getTableInfo(new TableIdent(qNameParts.get(0), qNameParts.get(1)));
                if (otherTable == null){
                    throw new TableUnknownException(qNameParts.get(0) + "." + qNameParts.get(1));
                }
                ident = new ReferenceIdent(new TableIdent(qNameParts.get(0), qNameParts.get(1)), qNameParts.get(2), path);
                break;
            default:
                throw new UnsupportedOperationException("unsupported name reference: " + name);
        }
        return ident;

    }

    /**
     * Compute an id and adds it, also add routing value
     *
     * @param primaryKeyValues
     */
    public void addIdAndRouting(List<String> primaryKeyValues, String clusteredByValue) {
        addIdAndRouting(false, primaryKeyValues, clusteredByValue);
    }

    protected void addIdAndRouting(Boolean create, List<String> primaryKeyValues, String clusteredByValue) {
        Id id = new Id(table().primaryKey(), primaryKeyValues, table().clusteredBy(), create);
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

    public List<String> ids() {
        return ids;
    }

    public List<String> routingValues() {
        return routingValues;
    }

    public boolean hasSysExpressions() {
        return hasSysExpressions;
    }

    @Override
    public <C, R> R accept(AnalysisVisitor<C, R> analysisVisitor, C context) {
        return analysisVisitor.visitDataAnalysis(this, context);
    }
}
