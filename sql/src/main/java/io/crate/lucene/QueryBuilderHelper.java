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

    public abstract Query eq(String columnName, Object value);
    public abstract Query lt(String columnName, Object value);
    public abstract Query lte(String columnName, Object value);
    public abstract Query gt(String columnName, Object value);
    public abstract Query gte(String columnName, Object value);
    public Query like(String columnName, Object value) {
        return eq(columnName, value);
    }

    static final class BooleanQueryBuilder extends QueryBuilderHelper {
        @Override
        public Query eq(String columnName, Object value) {
            return new TermQuery(new Term(columnName, value == true ? "T" : "F"));
        }

        @Override
        public Query lt(String columnName, Object value) {
            throw new UnsupportedOperationException("\"<\" operator doesn't work on boolean fields");
        }

        @Override
        public Query lte(String columnName, Object value) {
            throw new UnsupportedOperationException("\"<=\" operator doesn't work on boolean fields");
        }

        @Override
        public Query gt(String columnName, Object value) {
            throw new UnsupportedOperationException("\">\" operator doesn't work on boolean fields");
        }

        @Override
        public Query gte(String columnName, Object value) {
            throw new UnsupportedOperationException("\">=\" operator doesn't work on boolean fields");
        }
    }

    static final class FloatQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query eq(String columnName, Object value) {
            return NumericRangeQuery.newFloatRange(columnName, (Float)value, (Float)value, true, true);
        }

        @Override
        public Query lt(String columnName, Object value) {
            return NumericRangeQuery.newFloatRange(columnName, null, (Float)value, false, false);
        }

        @Override
        public Query lte(String columnName, Object value) {
            return NumericRangeQuery.newFloatRange(columnName, null, (Float)value, true, false);
        }

        @Override
        public Query gt(String columnName, Object value) {
            return NumericRangeQuery.newFloatRange(columnName, (Float)value, null, false, false);
        }

        @Override
        public Query gte(String columnName, Object value) {
            return NumericRangeQuery.newFloatRange(columnName, (Float)value, null, true, false);
        }
    }

    static final class DoubleQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query eq(String columnName, Object value) {
            return NumericRangeQuery.newDoubleRange(columnName, (Double) value, (Double) value, true, true);
        }

        @Override
        public Query lt(String columnName, Object value) {
            return NumericRangeQuery.newDoubleRange(columnName, null, (Double)value, false, false);
        }

        @Override
        public Query lte(String columnName, Object value) {
            return NumericRangeQuery.newDoubleRange(columnName, null, (Double)value, true, false);
        }

        @Override
        public Query gt(String columnName, Object value) {
            return NumericRangeQuery.newDoubleRange(columnName, (Double)value, null, false, false);
        }

        @Override
        public Query gte(String columnName, Object value) {
            return NumericRangeQuery.newDoubleRange(columnName, (Double)value, null, true, false);
        }
    }

    static final class LongQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query eq(String columnName, Object value) {
            return NumericRangeQuery.newLongRange(columnName, (Long)value, (Long)value, true, true);
        }

        @Override
        public Query lt(String columnName, Object value) {
            return NumericRangeQuery.newLongRange(columnName, null, (Long)value, false, false);
        }

        @Override
        public Query lte(String columnName, Object value) {
            return NumericRangeQuery.newLongRange(columnName, null, (Long)value, false, true);
        }

        @Override
        public Query gt(String columnName, Object value) {
            return NumericRangeQuery.newLongRange(columnName, (Long)value, null, false, false);
        }

        @Override
        public Query gte(String columnName, Object value) {
            return NumericRangeQuery.newLongRange(columnName, (Long)value, null, true, false);
        }
    }

    static final class IntegerQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query eq(String columnName, Object value) {
            return NumericRangeQuery.newIntRange(columnName, (Integer)value, (Integer)value, true, true);
        }

        @Override
        public Query lt(String columnName, Object value) {
            return NumericRangeQuery.newIntRange(columnName, null, (Integer) value, false, false);
        }

        @Override
        public Query lte(String columnName, Object value) {
            return NumericRangeQuery.newIntRange(columnName, null, (Integer) value, false, true);
        }

        @Override
        public Query gt(String columnName, Object value) {
            return NumericRangeQuery.newIntRange(columnName, (Integer)value, null, false, false);
        }

        @Override
        public Query gte(String columnName, Object value) {
            return NumericRangeQuery.newIntRange(columnName, (Integer)value, null, true, false);
        }
    }

    static final class StringQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query eq(String columnName, Object value) {
            return new TermQuery(new Term(columnName, (String)value));
        }

        @Override
        public Query lt(String columnName, Object value) {
            return new TermRangeQuery(columnName, null, BytesRefs.toBytesRef(value), false, false);
        }

        @Override
        public Query lte(String columnName, Object value) {
            return new TermRangeQuery(columnName, null, BytesRefs.toBytesRef(value), false, true);
        }

        @Override
        public Query gt(String columnName, Object value) {
            return new TermRangeQuery(columnName, BytesRefs.toBytesRef(value), null, false, false);
        }

        @Override
        public Query gte(String columnName, Object value) {
            return new TermRangeQuery(columnName, BytesRefs.toBytesRef(value), null, true, false);
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
