package org.cratedb.information_schema;

import org.apache.lucene.search.*;

public class InformationSchemaIntegerColumn extends InformationSchemaNumericColumn {

    public InformationSchemaIntegerColumn(String name) {
        this(name, false);
    }

    public InformationSchemaIntegerColumn(String name, boolean allowMultipleValues) {
        super(name, allowMultipleValues);
        type = SortField.Type.INT;
    }

    @Override
    public NumericRangeQuery<Integer> rangeQuery(Object from, Object to,
                                                 boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(name, (Integer)from, (Integer)to, includeLower, includeUpper);
    }

    @Override
    public MultiTermQueryWrapperFilter rangeFilter(
        Object from, Object to, boolean includeLower, boolean includeUpper)
    {
        return NumericRangeFilter.newIntRange(name, (Integer)from, (Integer)to, includeLower, includeUpper);
    }
}
