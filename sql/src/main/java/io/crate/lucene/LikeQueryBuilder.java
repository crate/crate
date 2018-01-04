/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

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
