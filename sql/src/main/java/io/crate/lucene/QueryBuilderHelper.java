package io.crate.lucene;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.cratedb.DataType;
import org.elasticsearch.common.lucene.BytesRefs;

public abstract class QueryBuilderHelper {

    private final static QueryBuilderHelper intQueryBuilder = new IntegerQueryBuilder();
    private final static QueryBuilderHelper longQueryBuilder = new LongQueryBuilder();
    private final static QueryBuilderHelper stringQueryBuilder = new StringQueryBuilder();
    private final static QueryBuilderHelper doubleQueryBuilder = new DoubleQueryBuilder();
    private final static QueryBuilderHelper floatQueryBuilder = new FloatQueryBuilder();
    private final static QueryBuilderHelper booleanQueryBuilder = new BooleanQueryBuilder();

    public static QueryBuilderHelper forType(DataType dataType) {
        switch (dataType) {
            case BYTE:
                return intQueryBuilder;
            case SHORT:
                return intQueryBuilder;
            case INTEGER:
                return intQueryBuilder;
            case TIMESTAMP:
            case LONG:
                return longQueryBuilder;
            case FLOAT:
                return floatQueryBuilder;
            case DOUBLE:
                return doubleQueryBuilder;
            case BOOLEAN:
                return booleanQueryBuilder;
            case IP:
            case STRING:
                return stringQueryBuilder;
            case OBJECT:
                break;
            case NOT_SUPPORTED:
                break;
            case NULL:
                break;
        }

        throw new UnsupportedOperationException(String.format("type %s not supported", dataType));
    }

    public abstract Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper);
    public abstract Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper);

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

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newFloatRange(columnName, (Float)from, (Float)to, includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newFloatRange(columnName, (Float)from, (Float)to, includeLower, includeUpper);
        }
    }

    static final class DoubleQueryBuilder extends QueryBuilderHelper {

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newDoubleRange(columnName, (Double) from, (Double) to, includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newDoubleRange(columnName, (Double)from, (Double)to, includeLower, includeUpper);
        }
    }

    static final class LongQueryBuilder extends QueryBuilderHelper {

        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newLongRange(columnName, (Long) from, (Long) to, includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newLongRange(columnName, (Long)from, (Long)to, includeLower, includeUpper);
        }
    }

    static final class IntegerQueryBuilder extends QueryBuilderHelper {
        @Override
        public Filter rangeFilter(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeFilter.newIntRange(columnName, (Integer) from, (Integer) to, includeLower, includeUpper);
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newIntRange(columnName, (Integer) from, (Integer) to, includeLower, includeUpper);
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
            return new TermQuery(new Term(columnName, (String)value));
        }

        @Override
        public Query like(String columnName, Object value) {
            String like = (String)value;

            // lucene uses * and ? as wildcard characters
            // but via SQL they are used as % and _
            // here they are converted back.
            like = like.replaceAll("(?<!\\\\)\\*", "\\\\*");
            like = like.replaceAll("(?<!\\\\)%", "*");
            like = like.replaceAll("\\\\%", "%");

            like = like.replaceAll("(?<!\\\\)\\?", "\\\\?");
            like = like.replaceAll("(?<!\\\\)_", "?");
            like = like.replaceAll("\\\\_", "_");

            return new WildcardQuery(new Term(columnName, like));
        }
    }
}
