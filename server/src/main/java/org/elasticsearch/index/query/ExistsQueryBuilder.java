/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.query;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.lucene.search.Queries;
import org.elasticsearch.index.mapper.FieldNamesFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.util.Collection;
import java.util.Collections;

/**
 * Constructs a query that only match on documents that the field has a value in them.
 */
public class ExistsQueryBuilder {
    public static final String NAME = "exists";

    public static Query newFilter(QueryShardContext context, String fieldPattern) {

        final FieldNamesFieldMapper.FieldNamesFieldType fieldNamesFieldType = (FieldNamesFieldMapper.FieldNamesFieldType) context
                .getMapperService().fullName(FieldNamesFieldMapper.NAME);
        if (fieldNamesFieldType == null) {
            // can only happen when no types exist, so no docs exist either
            return Queries.newMatchNoDocsQuery("Missing types in \"" + NAME + "\" query.");
        }

        final Collection<String> fields;
        if (context.getObjectMapper(fieldPattern) != null) {
            // the _field_names field also indexes objects, so we don't have to
            // do any more work to support exists queries on whole objects
            fields = Collections.singleton(fieldPattern);
        } else {
            fields = context.simpleMatchToIndexNames(fieldPattern);
        }

        if (fields.size() == 1) {
            String field = fields.iterator().next();
            return newFieldExistsQuery(context, field);
        }

        BooleanQuery.Builder boolFilterBuilder = new BooleanQuery.Builder();
        for (String field : fields) {
            boolFilterBuilder.add(newFieldExistsQuery(context, field), BooleanClause.Occur.SHOULD);
        }
        return new ConstantScoreQuery(boolFilterBuilder.build());
    }

    private static Query newFieldExistsQuery(QueryShardContext context, String field) {
        MappedFieldType fieldType = context.getMapperService().fullName(field);
        if (fieldType == null) {
            // The field does not exist as a leaf but could be an object so
            // check for an object mapper
            if (context.getObjectMapper(field) != null) {
                return newObjectFieldExistsQuery(context, field);
            }
            return Queries.newMatchNoDocsQuery("No field \"" + field + "\" exists in mappings.");
        }
        Query filter = fieldType.existsQuery(context);
        return new ConstantScoreQuery(filter);
    }

    private static Query newObjectFieldExistsQuery(QueryShardContext context, String objField) {
        BooleanQuery.Builder booleanQuery = new BooleanQuery.Builder();
        Collection<String> fields = context.simpleMatchToIndexNames(objField + ".*");
        for (String field : fields) {
            Query existsQuery = context.getMapperService().fullName(field).existsQuery(context);
            booleanQuery.add(existsQuery, Occur.SHOULD);
        }
        return new ConstantScoreQuery(booleanQuery.build());
    }
}
