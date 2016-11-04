package io.crate.lucene;

import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.WildcardQuery;
import org.elasticsearch.common.lucene.BytesRefs;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.MappedFieldType;

import javax.annotation.Nullable;

public final class LikeQueryBuilder {

    private LikeQueryBuilder() {
    }

    public static Query like(DataType dataType, @Nullable MappedFieldType fieldType, Object value) {
        if (fieldType == null) {
            // column doesn't exist on this index -> no match
            return Queries.newMatchNoDocsQuery("column does not exist in this index");
        }
        if (dataType.equals(DataTypes.STRING)) {
            return new WildcardQuery(new Term(
                fieldType.name(),
                LuceneQueryBuilder.convertSqlLikeToLuceneWildcard(BytesRefs.toString(value))));
        }
        return fieldType.termQuery(value, null);
    }
}
