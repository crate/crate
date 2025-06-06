/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.metadata.doc;

import static org.elasticsearch.cluster.metadata.Metadata.COLUMN_OID_UNASSIGNED;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;

import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.IndexOptions;

import io.crate.execution.engine.fetch.FetchId;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.SimpleReference;
import io.crate.types.DataType;
import io.crate.types.DataTypes;

public class SysColumns {


    public static class Names {
        public static final String ID = "_id";
        public static final String VERSION = "_version";
        public static final String SCORE = "_score";
        public static final String UID = "_uid";
        public static final String DOC = "_doc";
        public static final String RAW = "_raw";
        public static final String SEQ_NO = "_seq_no";
        public static final String PRIMARY_TERM = "_primary_term";
        public static final String TOMBSTONE = "_tombstone";

        /**
         * Column that contains the lucene docId + a readerId.
         * See {@link FetchId}
         */
        public static final String FETCHID = "_fetchid";
        public static final String DOCID = "_docid";
    }

    public static class ID {
        public static final ColumnIdent COLUMN = ColumnIdent.of(Names.ID);
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class Source {
        public static final String NAME = "_source";
        public static final String RECOVERY_NAME = "_recovery_source";

        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.NONE); // not indexed
            FIELD_TYPE.setStored(true);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static class FieldNames {
        public static final String NAME = "_field_names";
        public static final FieldType FIELD_TYPE = new FieldType();

        static {
            FIELD_TYPE.setIndexOptions(IndexOptions.DOCS);
            FIELD_TYPE.setTokenized(false);
            FIELD_TYPE.setStored(false);
            FIELD_TYPE.setOmitNorms(true);
            FIELD_TYPE.freeze();
        }
    }

    public static final ColumnIdent VERSION = ColumnIdent.of(Names.VERSION);
    public static final ColumnIdent SCORE = ColumnIdent.of(Names.SCORE);
    public static final ColumnIdent UID = ColumnIdent.of(Names.UID);
    public static final ColumnIdent DOC = ColumnIdent.of(Names.DOC);
    public static final ColumnIdent RAW = ColumnIdent.of(Names.RAW);
    public static final ColumnIdent SEQ_NO = ColumnIdent.of(Names.SEQ_NO);
    public static final ColumnIdent PRIMARY_TERM = ColumnIdent.of(Names.PRIMARY_TERM);

    /**
     * See {@link Names#FETCHID}
     */
    public static final ColumnIdent FETCHID = ColumnIdent.of(Names.FETCHID);
    public static final ColumnIdent DOCID = ColumnIdent.of(Names.DOCID);

    public static final Map<ColumnIdent, DataType<?>> COLUMN_IDENTS = Map.of(
        DOC, DataTypes.UNTYPED_OBJECT,
        FETCHID, DataTypes.LONG,
        ID.COLUMN, DataTypes.STRING,
        RAW, DataTypes.STRING,
        SCORE, DataTypes.FLOAT,
        UID, DataTypes.STRING,
        VERSION, DataTypes.LONG,
        DOCID, DataTypes.INTEGER,
        SEQ_NO, DataTypes.LONG,
        PRIMARY_TERM, DataTypes.LONG
    );

    private static final Map<ColumnIdent, String> LUCENE_COLUMN_NAMES = Map.of(
        RAW, "_source",
        ID.COLUMN, UID.name()
    );

    /**
     * Creates a Reference for a system column.
     * Don't use this for user table columns, it's not safe (e.g. Reference has no oid)
     */
    private static Reference newInfo(RelationName table, ColumnIdent column, DataType<?> dataType, int position) {
        return new SimpleReference(new ReferenceIdent(table, column),
                             RowGranularity.DOC,
                             dataType,
                             IndexType.PLAIN,
                             false,
                             false,
                             position,
                             COLUMN_OID_UNASSIGNED,
                             false,
                             null
        );
    }

    /**
     * Calls {@code consumer} for each sys column with a reference containing {@code relationName}
     */
    public static void forTable(RelationName relationName, BiConsumer<ColumnIdent, Reference> consumer) {
        int position = 1;
        for (Map.Entry<ColumnIdent, DataType<?>> entry : COLUMN_IDENTS.entrySet()) {
            ColumnIdent columnIdent = entry.getKey();
            consumer.accept(columnIdent, newInfo(relationName, columnIdent, entry.getValue(), position++));
        }
    }

    public static List<Reference> forTable(RelationName relationName) {
        int position = 1;
        var columns = new ArrayList<Reference>();
        for (Map.Entry<ColumnIdent, DataType<?>> entry : COLUMN_IDENTS.entrySet()) {
            columns.add(newInfo(relationName, entry.getKey(), entry.getValue(), position++));
        }
        return columns;
    }

    public static Reference forTable(RelationName table, ColumnIdent column) {
        return newInfo(table, column, COLUMN_IDENTS.get(column), 0);
    }

    public static String nameForLucene(ColumnIdent ident) {
        String name = LUCENE_COLUMN_NAMES.get(ident);
        if (name == null) {
            name = ident.name();
        }
        return name;
    }
}
