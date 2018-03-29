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

package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.execution.engine.fetch.FetchId;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.table.ColumnPolicy;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

import java.util.Map;
import java.util.function.BiConsumer;

public class DocSysColumns {


    public static class Names {
        public static final String ID = "_id";
        public static final String VERSION = "_version";
        public static final String SCORE = "_score";
        public static final String UID = "_uid";
        public static final String DOC = "_doc";
        public static final String RAW = "_raw";

        /**
         * Column that contains the lucene docId + a readerId.
         * See {@link FetchId}
         */
        public static final String FETCHID = "_fetchid";
    }

    public static final ColumnIdent ID = new ColumnIdent(Names.ID);
    public static final ColumnIdent VERSION = new ColumnIdent(Names.VERSION);
    public static final ColumnIdent SCORE = new ColumnIdent(Names.SCORE);
    public static final ColumnIdent UID = new ColumnIdent(Names.UID);
    public static final ColumnIdent DOC = new ColumnIdent(Names.DOC);
    public static final ColumnIdent RAW = new ColumnIdent(Names.RAW);

    /**
     * See {@link Names#FETCHID}
     */
    public static final ColumnIdent FETCHID = new ColumnIdent(Names.FETCHID);

    public static final ImmutableMap<ColumnIdent, DataType> COLUMN_IDENTS = ImmutableMap.<ColumnIdent, DataType>builder()
        .put(ID, DataTypes.STRING)
        .put(VERSION, DataTypes.LONG)
        .put(SCORE, DataTypes.FLOAT)
        .put(UID, DataTypes.STRING)
        .put(DOC, DataTypes.OBJECT)
        .put(RAW, DataTypes.STRING)
        .put(FETCHID, DataTypes.LONG)
        .build();

    private static final ImmutableMap<ColumnIdent, String> LUCENE_COLUMN_NAMES = ImmutableMap.<ColumnIdent, String>builder()
        .put(RAW, "_source")
        .put(ID, UID.name())
        .build();

    private static Reference newInfo(RelationName table, ColumnIdent column, DataType dataType) {
        return new Reference(new ReferenceIdent(table, column), RowGranularity.DOC, dataType, ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED, false);
    }

    /**
     * Calls {@code consumer} for each sys column with a reference containing {@code relationName}
     */
    public static void forTable(RelationName relationName, BiConsumer<ColumnIdent, Reference> consumer) {
        for (Map.Entry<ColumnIdent, DataType> entry : COLUMN_IDENTS.entrySet()) {
            ColumnIdent columnIdent = entry.getKey();
            consumer.accept(columnIdent, newInfo(relationName, columnIdent, entry.getValue()));
        }
    }

    public static Reference forTable(RelationName table, ColumnIdent column) {
        return newInfo(table, column, COLUMN_IDENTS.get(column));
    }

    public static String nameForLucene(ColumnIdent ident) {
        String name = LUCENE_COLUMN_NAMES.get(ident);
        if (name == null) {
            name = ident.name();
        }
        return name;
    }
}
