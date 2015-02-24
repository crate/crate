package io.crate.metadata.doc;

import com.google.common.collect.ImmutableMap;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.ReferenceInfo;
import io.crate.metadata.TableIdent;
import io.crate.planner.RowGranularity;
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
    public static final ColumnIdent DOCID = new ColumnIdent("_docid");

    public static final ImmutableMap<ColumnIdent, DataType> COLUMN_IDENTS = ImmutableMap.<ColumnIdent, DataType>builder()
            .put(ID, DataTypes.STRING)
            .put(VERSION, DataTypes.LONG)
            .put(SCORE, DataTypes.FLOAT)
            .put(UID, DataTypes.STRING)
            .put(DOC, DataTypes.OBJECT)
            .put(RAW, DataTypes.STRING)
            .put(DOCID, DataTypes.LONG)
            .build();

    private static ReferenceInfo newInfo(TableIdent table, ColumnIdent column, DataType dataType) {
        return new ReferenceInfo(new ReferenceIdent(table, column), RowGranularity.DOC, dataType);
    }

    public static List<Tuple<ColumnIdent, ReferenceInfo>> forTable(TableIdent tableIdent) {
        List<Tuple<ColumnIdent, ReferenceInfo>> columns = new ArrayList<>(COLUMN_IDENTS.size());

        for (Map.Entry<ColumnIdent, DataType> entry : COLUMN_IDENTS.entrySet()) {
            columns.add(new Tuple<>(entry.getKey(), newInfo(tableIdent, entry.getKey(), entry.getValue())));
        }

        return columns;
    }
}
