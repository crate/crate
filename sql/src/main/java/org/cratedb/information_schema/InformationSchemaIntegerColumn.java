package org.cratedb.information_schema;

import org.apache.lucene.search.NumericRangeQuery;
import org.apache.lucene.search.SortField;

public class InformationSchemaIntegerColumn extends InformationSchemaNumericColumn {

    public InformationSchemaIntegerColumn(String name) {
        super(name);
        type = SortField.Type.INT;
    }

    @Override
    public NumericRangeQuery<Integer> rangeQuery(Object from, Object to,
                                                 boolean includeLower, boolean includeUpper) {
        return NumericRangeQuery.newIntRange(name, (Integer)from, (Integer)to, includeLower, includeUpper);
    }
}
