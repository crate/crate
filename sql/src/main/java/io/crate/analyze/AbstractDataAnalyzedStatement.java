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
import io.crate.exceptions.*;
import io.crate.metadata.*;
import io.crate.metadata.sys.SysSchemaInfo;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.metadata.table.SchemaInfo;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.*;
import org.apache.lucene.util.BytesRef;

import javax.annotation.Nullable;
import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public abstract class AbstractDataAnalyzedStatement extends AnalyzedStatement {

    protected static final Predicate<ReferenceInfo> IS_OBJECT_ARRAY = new Predicate<ReferenceInfo>() {
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
    protected final Functions functions;
    protected final ReferenceResolver referenceResolver;
    protected TableInfo table;
    protected final List<String> ids = new ArrayList<>();
    protected final List<String> routingValues = new ArrayList<>();

    private Map<Function, Function> functionSymbols = new HashMap<>();

    protected Map<ReferenceIdent, Reference> referenceSymbols = new HashMap<>();

    protected List<Symbol> outputSymbols = ImmutableList.of();

    protected WhereClause whereClause = WhereClause.MATCH_ALL;
    protected boolean hasAggregates = false;
    protected boolean hasSysExpressions = false;
    protected boolean sysExpressionsAllowed = false;
    protected boolean insideNotPredicate = false;
    private String tableAlias;

    public AbstractDataAnalyzedStatement(ReferenceInfos referenceInfos,
                                         Functions functions,
                                         ParameterContext parameterContext,
                                         ReferenceResolver referenceResolver) {
        super(parameterContext);
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.referenceResolver = referenceResolver;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

    public void tableAlias(String tableAlias) {
        this.tableAlias = tableAlias;
    }

    public String tableAlias() {
        return this.tableAlias;
    }

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
        table = tableInfo;
    }

    public TableInfo table() {
        return this.table;
    }

    private Reference allocateReference(ReferenceIdent ident, boolean unique, boolean forWrite) {
        String schema = ident.tableIdent().schema();
        if (schema != null && schema.equals(SysSchemaInfo.NAME)) {
            hasSysExpressions = true;
        }
        Reference reference = referenceSymbols.get(ident);
        if (reference == null) {
            ReferenceInfo info = getReferenceInfo(ident);
            if (info == null) {
                info = table.indexColumn(ident.columnIdent());
                if (info == null) {
                    TableInfo tableInfo = table;
                    if (ident.tableIdent() != table.ident()) {
                        tableInfo = referenceInfos.getTableInfo(ident.tableIdent());
                    }
                    reference = tableInfo.getDynamic(ident.columnIdent(), forWrite);
                    if (reference == null) {
                        throw new ColumnUnknownException(ident.columnIdent().fqn());
                    }
                    info = reference.info();
                }
            }
            if (info.granularity().finerThan(table.rowGranularity())) {
                throw new UnsupportedOperationException(String.format(Locale.ENGLISH,
                        "Cannot resolve reference '%s.%s', reason: finer granularity than table '%s'",
                        info.ident().tableIdent().fqn(), info.ident().columnIdent().fqn(), table.ident().fqn()));
            }
            if (reference == null) {
                reference = new Reference(info);
            }
            referenceSymbols.put(info.ident(), reference);
        } else if (unique) {
            throw new IllegalArgumentException(String.format(Locale.ENGLISH, "reference '%s' repeated", ident.columnIdent().fqn()));
        }
        return reference;
    }

    /**
     * add a reference for the given ident, get from map-cache if already
     */
    public Reference allocateReference(ReferenceIdent ident) {
        return allocateReference(ident, false, false);
    }

    public Reference allocateReference(ReferenceIdent ident, boolean forWrite) {
        return allocateReference(ident, false, forWrite);
    }

    /**
     * add a new reference for the given ident
     * and throw an error if this ident has already been added
     */
    public Reference allocateUniqueReference(ReferenceIdent ident, boolean forWrite) {
        return allocateReference(ident, true, forWrite);
    }

    @Nullable
    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceInfos.getReferenceInfo(ident);
        if (info != null &&
                !info.ident().isColumn() &&
                hasMatchingParent(info, IS_OBJECT_ARRAY)) {
            if (DataTypes.isCollectionType(info.type())) {
                // TODO: remove this limitation with next type refactoring
                throw new UnsupportedOperationException(
                        "cannot query for arrays inside object arrays explicitly");
            }

            // for child fields of object arrays
            // return references of primitive types as arrays
            info = new ReferenceInfo.Builder()
                    .ident(info.ident())
                    .columnPolicy(info.columnPolicy())
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
     */
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
        return functionSymbols.values();
    }

    public Function allocateFunction(FunctionInfo info, List<Symbol> arguments) {
        Function function = new Function(info, arguments);
        Function existing = functionSymbols.get(function);
        if (existing != null) {
            return existing;
        } else {
            if (info.type() == FunctionInfo.Type.AGGREGATE){
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
        this.whereClause = whereClause;
        return this.whereClause;
    }

    public WhereClause whereClause() {
        return whereClause;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public void outputSymbols(List<Symbol> symbols) {
        this.outputSymbols = symbols;
        this.outputTypes = Symbols.extractTypes(symbols);
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.planner.symbol.Reference}
     *
     * Replaced with {@link io.crate.analyze.expressions.ExpressionAnalyzer}
     *
     * @param valueSymbol the value to normalize, might be anything from {@link io.crate.metadata.Scalar} to {@link io.crate.planner.symbol.Literal}
     * @param reference  the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws io.crate.exceptions.ColumnValidationException
     */
    @Deprecated
    public Literal normalizeInputForReference(Symbol valueSymbol, Reference reference, boolean forWrite) {
        Literal literal;
        try {
            literal = (Literal) normalizer.process(valueSymbol, null);
            if (reference instanceof DynamicReference) {
                if (reference.info().columnPolicy() != ColumnPolicy.IGNORED) {
                    // re-guess without strict to recognize timestamps
                    DataType<?> dataType = DataTypes.guessType(literal.value(), false);
                    validateInputType(dataType, reference.info().ident().columnIdent());
                    ((DynamicReference) reference).valueType(dataType);
                    literal = Literal.convert(literal, dataType); // need to update literal if the type changed
                } else {
                    ((DynamicReference) reference).valueType(literal.valueType());
                }
            } else {
                validateInputType(literal.valueType(), reference.info().ident().columnIdent());
                literal = Literal.convert(literal, reference.valueType());
            }
        } catch (ClassCastException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format(Locale.ENGLISH, "Invalid value of type '%s'", valueSymbol.symbolType().name()));
        }

        try {
            // 3. if reference is of type object - do special validation
            if (reference.info().type() == DataTypes.OBJECT) {
                @SuppressWarnings("unchecked")
                Map<String, Object> value = (Map<String, Object>) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(normalizeObjectValue(value, reference.info(), forWrite));
            } else if (isObjectArray(reference.info().type())) {
                Object[] value = (Object[]) literal.value();
                if (value == null) {
                    return Literal.NULL;
                }
                literal = Literal.newLiteral(
                        reference.info().type(),
                        normalizeObjectArrayValue(value, reference.info())
                );
            }
        } catch (ClassCastException | NumberFormatException e) {
            throw new ColumnValidationException(
                    reference.info().ident().columnIdent().name(),
                    SymbolFormatter.format(
                            "\"%s\" has a type that can't be implicitly cast to that of \"%s\" (" + reference.valueType().getName() + ")",
                            literal,
                            reference
                    ));
        }
        return literal;
    }

    /**
     * validate input types to not be nested arrays/collection types
     * @throws ColumnValidationException if input type is a nested array type
     */
    private void validateInputType(DataType dataType, ColumnIdent columnIdent) throws ColumnValidationException {
        if (dataType != null
                && DataTypes.isCollectionType(dataType)
                && DataTypes.isCollectionType(((CollectionType)dataType).innerType())) {
            throw new ColumnValidationException(columnIdent.fqn(),
                    String.format(Locale.ENGLISH, "Invalid datatype '%s'", dataType));
        }
    }

    public Literal normalizeInputForReference(Symbol inputValue, Reference reference) {
        return normalizeInputForReference(inputValue, reference, false);
    }

    private boolean isObjectArray(DataType type) {
        return type.id() == ArrayType.ID && ((ArrayType)type).innerType().id() == ObjectType.ID;
    }

    /**
     * normalize and validate the given value according to the given {@link io.crate.types.DataType}
     *
     * Replaced with {@link io.crate.analyze.expressions.ExpressionAnalyzer}
     *
     * @param inputValue any {@link io.crate.planner.symbol.Symbol} that evaluates to a Literal or Parameter
     * @param dataType the type to convert this input to
     * @return a {@link io.crate.planner.symbol.Literal} of type <code>dataType</code>
     */
    @Deprecated
    public Literal normalizeInputForType(Symbol inputValue, DataType dataType) {
        try {
            return Literal.convert(normalizer.process(inputValue, null), dataType);
        } catch (ClassCastException | NumberFormatException e) {
            throw new IllegalArgumentException(
                    String.format(Locale.ENGLISH, "Invalid value of type '%s'", inputValue.symbolType().name()));
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeObjectValue(Map<String, Object> value, ReferenceInfo info, boolean forWrite) {
        for (Map.Entry<String, Object> entry : value.entrySet()) {
            ColumnIdent nestedIdent = ColumnIdent.getChild(info.ident().columnIdent(), entry.getKey());
            ReferenceInfo nestedInfo = table.getReferenceInfo(nestedIdent);
            if (nestedInfo == null) {
                if (info.columnPolicy() == ColumnPolicy.IGNORED) {
                    continue;
                }
                TableInfo tableInfo = table;
                if (info.ident().tableIdent() != table.ident()) {
                    tableInfo = referenceInfos.getTableInfo(info.ident().tableIdent());
                }
                DynamicReference dynamicReference = tableInfo.getDynamic(nestedIdent, forWrite);
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
                value.put(entry.getKey(), normalizeObjectValue((Map<String, Object>) entry.getValue(), nestedInfo, forWrite));
            } else if (isObjectArray(nestedInfo.type()) && entry.getValue() instanceof Object[]) {
                value.put(entry.getKey(), normalizeObjectArrayValue((Object[])entry.getValue(), nestedInfo, forWrite));
            } else {
                value.put(entry.getKey(), normalizePrimitiveValue(entry.getValue(), nestedInfo));
            }
        }
        return value;
    }

    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo) {
        return normalizeObjectArrayValue(value, arrayInfo, false);
    }

    private Object[] normalizeObjectArrayValue(Object[] value, ReferenceInfo arrayInfo, boolean forWrite) {
        for (Object arrayItem : value) {
            Preconditions.checkArgument(arrayItem instanceof Map, "invalid value for object array type");
            arrayItem = normalizeObjectValue((Map<String, Object>)arrayItem, arrayInfo, forWrite);
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
        this.outputTypes = Symbols.extractTypes(outputSymbols());
        if (whereClause().hasQuery()) {
            whereClause.normalize(normalizer);
            if (onlyScalarsAllowed && whereClause().hasQuery()){
                for (Function function : functionSymbols.keySet()) {
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
     * get a reference from a given {@link io.crate.sql.tree.QualifiedName}
     */
    protected ReferenceIdent getReference(QualifiedName name) {
        return getReference(name, ImmutableList.<String>of());
    }

    protected ReferenceIdent getReference(QualifiedName name, List<String> path) {
        Preconditions.checkState(table.ident() != null);
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
     */
    public void addIdAndRouting(List<BytesRef> primaryKeyValues, String clusteredByValue) {
        addIdAndRouting(false, primaryKeyValues, clusteredByValue);
    }

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
        return hasSysExpressions;
    }

    @Override
    public <C, R> R accept(AnalyzedStatementVisitor<C, R> analyzedStatementVisitor, C context) {
        return analyzedStatementVisitor.visitDataAnalyzedStatement(this, context);
    }
}
