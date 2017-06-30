package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
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
         * See {@link io.crate.operation.projectors.fetch.FetchId}
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

    private static Reference newInfo(TableIdent table, ColumnIdent column, DataType dataType) {
        return new Reference(new ReferenceIdent(table, column), RowGranularity.DOC, dataType, ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED, false);
    }

    /**
     * Calls {@code consumer} for each sys column with a reference containing {@code tableIdent}
     */
    public static void forTable(TableIdent tableIdent, BiConsumer<ColumnIdent, Reference> consumer) {
        for (Map.Entry<ColumnIdent, DataType> entry : COLUMN_IDENTS.entrySet()) {
            ColumnIdent columnIdent = entry.getKey();
            consumer.accept(columnIdent, newInfo(tableIdent, columnIdent, entry.getValue()));
        }
    }

    public static Reference forTable(TableIdent table, ColumnIdent column) {
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
