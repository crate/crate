package io.crate.analyze;

import com.google.common.collect.ImmutableList;
import io.crate.metadata.*;
import io.crate.metadata.table.TableInfo;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.cratedb.DataType;
import org.cratedb.sql.ValidationException;
import org.elasticsearch.common.Preconditions;

import javax.annotation.Nullable;
import java.util.*;


/**
 * Holds information the analyzer has gathered about a statement.
 */
public abstract class Analysis {

    protected final EvaluatingNormalizer normalizer;

    public static enum Type {
        SELECT,
        INSERT
    }

    public abstract Type type();

    protected final ReferenceInfos referenceInfos;
    private final Functions functions;
    private final Object[] parameters;
    protected TableInfo table;

    private Map<Function, Function> functionSymbols = new HashMap<>();

    protected Map<ReferenceIdent, Reference> referenceSymbols = new IdentityHashMap<>();

    private List<String> outputNames = ImmutableList.of();
    private List<Symbol> outputSymbols = ImmutableList.of();

    protected boolean noMatch = false;
    protected List<Literal> primaryKeyLiterals;
    protected Literal clusteredByLiteral;

    private boolean isDelete = false;
    protected Function whereClause;
    protected RowGranularity rowGranularity;
    protected boolean hasAggregates = false;

    public List<Literal> primaryKeyLiterals() {
        return primaryKeyLiterals;
    }

    public void primaryKeyLiterals(List<Literal> primaryKeyLiterals) {
        this.primaryKeyLiterals = primaryKeyLiterals;
    }

    @Nullable
    public Literal clusteredByLiteral(){
        return clusteredByLiteral;
    }

    public void clusteredByLiteral(Literal clusteredByLiteral){
        this.clusteredByLiteral = clusteredByLiteral;
    }

    public Analysis(ReferenceInfos referenceInfos, Functions functions, Object[] parameters,
                    ReferenceResolver referenceResolver) {
        this.referenceInfos = referenceInfos;
        this.functions = functions;
        this.parameters = parameters;
        this.normalizer = new EvaluatingNormalizer(functions, RowGranularity.CLUSTER, referenceResolver);
    }

    public void table(TableIdent tableIdent) {
        table = referenceInfos.getTableInfo(tableIdent);
        Preconditions.checkNotNull(table, "Table not found", tableIdent);
        updateRowGranularity(table.rowGranularity());
    }

    public TableInfo table() {
        return this.table;
    }

    public Reference allocateReference(ReferenceIdent ident) {
        Reference reference = referenceSymbols.get(ident);
        if (reference == null) {
            ReferenceInfo info = getReferenceInfo(ident);
            reference = new Reference(info);
            referenceSymbols.put(info.ident(), reference);
        }
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    /**
     * add a new reference for the given ident
     * and throw an error if this ident has already been added
     */
    public Reference allocateUniqueReference(ReferenceIdent ident) {
        if (referenceSymbols.get(ident) != null) {
            throw new IllegalArgumentException(String.format("reference '%s' repeated", ident));
        }
        ReferenceInfo info = getReferenceInfo(ident);
        Reference reference = new Reference(info);
        referenceSymbols.put(info.ident(), reference);
        updateRowGranularity(reference.info().granularity());
        return reference;
    }

    public ReferenceInfo getReferenceInfo(ReferenceIdent ident) {
        ReferenceInfo info = referenceInfos.getReferenceInfo(ident);
        if (info == null) {
            throw new UnsupportedOperationException("TODO: unknown column reference: " + ident);
        }
        return info;
    }

    public FunctionInfo getFunctionInfo(FunctionIdent ident) {
        FunctionImplementation implementation = functions.get(ident);
        if (implementation == null) {
            throw new UnsupportedOperationException("TODO: unknown function? " + ident.toString());
        }
        return implementation.info();
    }

    public Collection<Reference> references() {
        return referenceSymbols.values();
    }

    public Collection<Function> functions() {
        return functionSymbols.values();
    }

    public Function allocateFunction(FunctionInfo info, List<Symbol> arguments) {
        if (info.isAggregate()) {
            hasAggregates = true;
        }
        Function function = new Function(info, arguments);
        Function existing = functionSymbols.get(function);
        if (existing != null) {
            return existing;
        } else {
            functionSymbols.put(function, function);
        }
        return function;
    }

    /**
     * Indicates that the statement will not match, so that there is no need to execute it
     */
    public boolean noMatch() {
        return noMatch;
    }

    public void noMatch(boolean noMatch) {
        this.noMatch = noMatch;
    }

    @Nullable
    public Function whereClause(@Nullable Symbol whereClause) {
        if (whereClause != null) {
            Symbol normalizedWhereClause = normalizer.process(whereClause, null);
            switch (normalizedWhereClause.symbolType()) {
                case FUNCTION:
                    this.whereClause = (Function) normalizedWhereClause;
                    break;
                case BOOLEAN_LITERAL:
                    noMatch = !((BooleanLiteral) normalizedWhereClause).value();
                    break;
                case NULL_LITERAL:
                    noMatch = true;
                default:
                    throw new UnsupportedOperationException("unsupported whereClause symbol: " + normalizedWhereClause);
            }
        }
        return this.whereClause;
    }

    public Function whereClause() {
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

    public Object parameterAt(int idx) {
        Preconditions.checkElementIndex(idx, parameters.length);
        return parameters[idx];
    }

    public void addOutputName(String s) {
        this.outputNames.add(s);
    }

    public void outputNames(List<String> outputNames) {
        this.outputNames = outputNames;
    }

    public List<String> outputNames() {
        return outputNames;
    }

    public List<Symbol> outputSymbols() {
        return outputSymbols;
    }

    public void outputSymbols(List<Symbol> symbols) {
        this.outputSymbols = symbols;
    }

    public boolean isDelete() {
        return isDelete;
    }

    public void isDelete(boolean isDelete) {
        this.isDelete = isDelete;
    }

    /**
     * normalize and validate given value according to the corresponding {@link io.crate.planner.symbol.Reference}
     * @param inputValue the value to normalize, might be anything from {@link io.crate.metadata.Scalar} to {@link io.crate.planner.symbol.Literal}
     * @param reference the reference to which the value has to comply in terms of type-compatibility
     * @return the normalized Symbol, should be a literal
     * @throws org.cratedb.sql.ValidationException
     */
    public Literal normalizeInputValue(Symbol inputValue, Reference reference) {
        Literal normalized;
        // 1. evaluate
        try {
            // everything that is allowed for input should evaluate to Literal
            normalized = (Literal)normalizer.process(inputValue, null);
        } catch (ClassCastException e) {
            throw new ValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format("Invalid value '%s'", inputValue.symbolType().name()));
        }

        // 2. convert if necessary (detect wrong types)
        try {
            normalized = normalized.convertTo(reference.info().type());
        } catch (Exception e) {  // UnsupportedOperationException, NumberFormatException ...
            throw new ValidationException(
                    reference.info().ident().columnIdent().name(),
                    String.format("wrong type '%s'. expected: '%s'",
                            normalized.valueType().getName(),
                            reference.info().type().getName()));
        }

        // 3. if reference is of type object - do special validation
        if (reference.info().type() == DataType.OBJECT
                && normalized instanceof ObjectLiteral) {
            Map<String, Object> value = ((ObjectLiteral)normalized).value();
            if (value == null) {
                return Null.INSTANCE;
            }
            normalized = new ObjectLiteral(normalizeObjectValue(value, reference.info()));
        }

        return normalized;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> normalizeObjectValue(Map<String, Object> value, ReferenceInfo referenceInfo) {
        if (referenceInfo.objectType() == ReferenceInfo.ObjectType.STRICT && value.size() > referenceInfo.nestedColumns().size()) {
            throw new ValidationException(referenceInfo.ident().columnIdent().fqn(), "cannot add new columns to STRICT object");
        }
        for (ReferenceInfo info : referenceInfo.nestedColumns()) {
            List<String> path = info.ident().columnIdent().path();
            String mapKey = path.get(path.size()-1);
            Object nestedValue = value.get(mapKey);
            if (nestedValue == null) {
                continue;
            }
            if (info.type() == DataType.OBJECT && nestedValue instanceof Map) {
                value.put(mapKey, normalizeObjectValue((Map<String, Object>)nestedValue, info));
            } else {
                value.put(mapKey, normalizePrimitiveValue(nestedValue, info));
            }
        }
        return value;
    }

    private Object normalizePrimitiveValue(Object primitiveValue, ReferenceInfo info) {
        try {
            // try to convert to correctly typed literal
            Literal l = Literal.forValue(primitiveValue);
            return l.convertTo(info.type()).value();
        } catch (Exception e) {
            throw new ValidationException(info.ident().columnIdent().fqn(),
                    String.format("Validation failed for %s: Invalid %s",
                            info.ident().columnIdent().fqn(),
                            info.type().getName()
                            )
                    );
        }
    }
}
