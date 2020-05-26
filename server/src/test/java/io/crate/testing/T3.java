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

package io.crate.testing;

import com.google.common.collect.ImmutableMap;
import io.crate.analyze.relations.AnalyzedRelation;
import io.crate.analyze.relations.DocTableRelation;
import io.crate.metadata.RelationName;
import io.crate.metadata.Schemas;
import org.elasticsearch.cluster.service.ClusterService;

import java.io.IOException;
import java.util.Map;

public class T3 {

    public static final String T1_DEFINITION =
        "create table doc.t1 (" +
        "  a text," +
        "  x int," +
        "  i int" +
        ")";

    public static final String T2_DEFINITION =
        "create table doc.t2 (" +
        "  b text," +
        "  y int," +
        "  i int" +
        ")";

    public static final String T3_DEFINITION =
        "create table doc.t3 (" +
        "  c text," +
        "  z int" +
        ")";

    public static final String T4_DEFINITION =
        "create table doc.t4 (" +
        "  id int," +
        "  obj object as (" +
        "    i int" +
        "  )," +
        "  obj_array array(object as (" +
        "    i int" +
        "  ))" +
        ")";

    public static final String T5_DEFINITION =
        "create table t5 (" +
        "  i int," +
        "  w bigint," +
        "  ts_z timestamp with time zone," +
        "  ts timestamp without time zone" +
        ")";

    public static final RelationName T1 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t1");
    public static final RelationName T2 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t2");
    public static final RelationName T3 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t3");
    public static final RelationName T4 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t4");
    public static final RelationName T5 = new RelationName(Schemas.DOC_SCHEMA_NAME, "t5");

    private static final Map<RelationName, String> RELATION_DEFINITIONS = ImmutableMap.of(
        T1, T1_DEFINITION,
        T2, T2_DEFINITION,
        T3, T3_DEFINITION,
        T4, T4_DEFINITION,
        T5, T5_DEFINITION);

    public static Map<RelationName, AnalyzedRelation> sources(ClusterService clusterService) {
        return sources(RELATION_DEFINITIONS.keySet(), clusterService);
    }

    public static Map<RelationName, AnalyzedRelation> sources(Iterable<RelationName> relations, ClusterService clusterService) {
        SQLExecutor.Builder executorBuilder = SQLExecutor.builder(clusterService);
        relations.forEach(rn -> {
            String tableDefinition = RELATION_DEFINITIONS.get(rn);
            if (tableDefinition == null) {
                throw new RuntimeException("Unknown relation " + rn);
            }
            try {
                executorBuilder.addTable(tableDefinition);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        SQLExecutor executor = executorBuilder.build();
        Schemas schemas = executor.schemas();

        ImmutableMap.Builder<RelationName, AnalyzedRelation> builder = ImmutableMap.builder();
        for (RelationName relationName : relations) {
            builder.put(
                relationName,
                new DocTableRelation(schemas.getTableInfo(relationName)));
        }
        return builder.build();
    }
}
