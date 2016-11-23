package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.*;
import io.crate.types.DataType;
import io.crate.types.DataTypes;
import org.elasticsearch.common.collect.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DocSysColumns {


    public static final ColumnIdent ID = new ColumnIdent("_id");
    public static final ColumnIdent VERSION = new ColumnIdent("_version");
    public static final ColumnIdent SCORE = new ColumnIdent("_score");
    public static final ColumnIdent UID = new ColumnIdent("_uid");
    public static final ColumnIdent DOC = new ColumnIdent("_doc");
    public static final ColumnIdent RAW = new ColumnIdent("_raw");
    public static final ColumnIdent FETCHID = new ColumnIdent("_fetchid");

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
        return new Reference(new ReferenceIdent(table, column), RowGranularity.DOC, dataType);
    }

    public static List<Tuple<ColumnIdent, Reference>> forTable(TableIdent tableIdent) {
        List<Tuple<ColumnIdent, Reference>> columns = new ArrayList<>(COLUMN_IDENTS.size());
        for (Map.Entry<ColumnIdent, DataType> entry : COLUMN_IDENTS.entrySet()) {
            columns.add(new Tuple<>(entry.getKey(), newInfo(tableIdent, entry.getKey(), entry.getValue())));
        }
        return columns;
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
