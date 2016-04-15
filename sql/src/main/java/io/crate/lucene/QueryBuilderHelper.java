package io.crate.lucene;

import io.crate.types.CollectionType;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.*;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.apache.lucene.util.NumericUtils;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.ip.IpFieldMapper;

import javax.annotation.Nullable;
import java.util.Locale;

public abstract class QueryBuilderHelper {

    private final static QueryBuilderHelper intQueryBuilder = new IntegerQueryBuilder();
    private final static QueryBuilderHelper longQueryBuilder = new LongQueryBuilder();
    private final static QueryBuilderHelper stringQueryBuilder = new StringQueryBuilder();
    private final static QueryBuilderHelper ipQueryBuilder = new IpQueryBuilder();
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
        if (dataType.equals(DataTypes.IP)) {
            return ipQueryBuilder;
        }
        if (dataType.equals(DataTypes.STRING)) {
            return stringQueryBuilder;
        }
        throw new UnsupportedOperationException(String.format(Locale.ENGLISH, "type %s not supported", dataType));
    }

    public abstract Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper);

    public Query eq(String columnName, Object value) {
        if (value == null) {
            return Queries.newMatchNoDocsQuery();
        }
        return rangeQuery(columnName, value, value, true, true);
    }

    public Query like(String columnName, Object value, @Nullable QueryCache queryCache) {
        return eq(columnName, value);
    }

    static final class BooleanQueryBuilder extends QueryBuilderHelper {
        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            throw new UnsupportedOperationException("This type of comparison is not supported on boolean fields");
        }

        @Override
        public Query eq(String columnName, Object value) {
            if (value == null) {
                return Queries.newMatchNoDocsQuery();
            }
            return new TermQuery(new Term(columnName, (boolean)value ? "T" : "F"));
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
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newIntRange(columnName, toInt(from), toInt(to), includeLower, includeUpper);
        }
    }

    static final class IpQueryBuilder extends QueryBuilderHelper {

        private BytesRef valueForSearch(Object value) {
            if (value == null) return null;
            BytesRefBuilder bytesRef = new BytesRefBuilder();
            NumericUtils.longToPrefixCoded(parseValue(value), 0, bytesRef); // 0 because of exact match
            return bytesRef.get();
        }

        private Long parseValueOrNull(Object value) {
            return value == null ? null : parseValue(value);
        }

        private long parseValue(Object value) {
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            if (value instanceof BytesRef) {
                return IpFieldMapper.ipToLong(((BytesRef) value).utf8ToString());
            }
            return IpFieldMapper.ipToLong(value.toString());
        }

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return NumericRangeQuery.newLongRange(columnName, parseValueOrNull(from), parseValueOrNull(to), includeLower, includeUpper);
        }

        @Override
        public Query eq(String columnName, Object value) {
            if (value == null) {
                return Queries.newMatchNoDocsQuery();
            }
            return new TermQuery(new Term(columnName, valueForSearch(value)));
        }
    }

    static final class StringQueryBuilder extends QueryBuilderHelper {

        @Override
        public Query rangeQuery(String columnName, Object from, Object to, boolean includeLower, boolean includeUpper) {
            return new TermRangeQuery(columnName, BytesRefs.toBytesRef(from), BytesRefs.toBytesRef(to), includeLower, includeUpper);
        }

        @Override
        public Query eq(String columnName, Object value) {
            if (value == null) {
                return Queries.newMatchNoDocsQuery();
            }
            return new TermQuery(new Term(columnName, (BytesRef)value));
        }

        @Override
        public Query like(String columnName, Object value, @Nullable QueryCache queryCache) {
            return new WildcardQuery(new Term(columnName, LuceneQueryBuilder.convertSqlLikeToLuceneWildcard(BytesRefs.toString(value))));
        }
    }
}
