package io.crate.lucene;

import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.lucene.BytesRefs;

public abstract class QueryBuilderHelper {

    private final static QueryBuilderHelper intQueryBuilder = new IntegerQueryBuilder();
    private final static QueryBuilderHelper longQueryBuilder = new LongQueryBuilder();
    private final static QueryBuilderHelper stringQueryBuilder = new StringQueryBuilder();
    private final static QueryBuilderHelper doubleQueryBuilder = new DoubleQueryBuilder();
    private final static QueryBuilderHelper floatQueryBuilder = new FloatQueryBuilder();
    private final static QueryBuilderHelper booleanQueryBuilder = new BooleanQueryBuilder();

    public static QueryBuilderHelper forType(DataType dataType) {
        while (dataType instanceof CollectionType) {
            dataType = ((CollectionType) dataType).innerType();
        }
        if (dataType.equals(DataTypes.BOOLEAN)) {
            return booleanQueryBuilder;
        }
        if (dataType.equals(DataTypes.BYTE)) {
            return intQueryBuilder;
        }
        if (dataType.equals(DataTypes.SHORT)) {
            return intQueryBuilder;
        }
        if (dataType.equals(DataTypes.INTEGER)) {
            return intQueryBuilder;
        }
        if (dataType.equals(DataTypes.TIMESTAMP) || dataType.equals(DataTypes.LONG)) {
            return longQueryBuilder;
        }
        if (dataType.equals(DataTypes.FLOAT)) {
            return floatQueryBuilder;
        }
        if (dataType.equals(DataTypes.DOUBLE)) {
            return doubleQueryBuilder;
        }
        if (dataType.equals(DataTypes.IP) || dataType.equals(DataTypes.STRING)) {
            return stringQueryBuilder;
        }
        throw new UnsupportedOperationException(String.format("type %s not supported", dataType));
    }

    public abstract Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper);
    public abstract Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper);

    public Filter eqFilter(String columnName, Object value) {
        return rangeFilter(columnName, value, value, true, true);
    }

    public Query eq(String columnName, Object value) {
        return rangeQuery(columnName, value, value, true, true);
    }

    public Query like(String columnName, Object value) {
        return eq(columnName, value);
    }

    static final class BooleanQueryBuilder extends QueryBuilderHelper {
        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            throw new UnsupportedOperationException("This type of comparison is not supported on boolean fields");
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            throw new UnsupportedOperationException("This type of comparison is not supported on boolean fields");
        }

        @Override
        public Query eq(String columnName, Object value) {
            return new TermQuery(new Term(columnName, value == true ? "T" : "F"));
        }
    }

    static final class FloatQueryBuilder extends QueryBuilderHelper {

        static Float toFloat(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Float) {
                return (Float) value;
            }
            return ((Number) value).floatValue();
        }

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newFloatRange(columnName, toFloat(from), toFloat(to), includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newFloatRange(columnName, toFloat(from), toFloat(to), includeLower, includeUpper);
        }
    }

    static final class DoubleQueryBuilder extends QueryBuilderHelper {

        static Double toDouble(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Double) {
                return (Double) value;
            }
            return ((Number) value).doubleValue();
        }

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newDoubleRange(columnName, toDouble(from), toDouble(to), includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newDoubleRange(columnName, toDouble(from), toDouble(to), includeLower, includeUpper);
        }
    }

    static final class LongQueryBuilder extends QueryBuilderHelper {

        static Long toLong(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Long) {
                return (Long) value;
            }
            return ((Number) value).longValue();
        }

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newLongRange(columnName, toLong(from), toLong(to), includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newLongRange(columnName, (Long)from, (Long)to, includeLower, includeUpper);
        }
    }

    static final class IntegerQueryBuilder extends QueryBuilderHelper {

        static Integer toInt(Object value) {
            if (value == null) {
                return null;
            }
            if (value instanceof Integer) {
                return (Integer) value;
            }
            return ((Number) value).intValue();
        }

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newIntRange(columnName, toInt(from), toInt(to), includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newIntRange(columnName, toInt(from), toInt(to), includeLower, includeUpper);
        }
    }

    static final class StringQueryBuilder extends QueryBuilderHelper {

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return new TermRangeFilter(columnName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return new TermRangeQuery(columnName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }

        @Override
        public Query eq(String columnName, Object value) {
            return new TermQuery(new Term(columnName, (BytesRef)value));
        }

        @Override
        public Filter eqFilter(String columnName, Object value) {
            return new TermFilter(new Term(columnName, (BytesRef) value));
        }

        @Override
        public Query like(String columnName, Object value) {
            return new WildcardQuery(
                    new Term(columnName, LuceneQueryBuilder.convertWildcard(BytesRefs.toString(value))));
        }
    }
}
