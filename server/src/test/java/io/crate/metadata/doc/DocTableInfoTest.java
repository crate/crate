package io.crate.metadata.doc;

import com.google.common.collect.ImmutableList;
import io.crate.expression.symbol.DynamicReference;
import io.crate.metadata.ColumnIdent;
import io.crate.metadata.Reference;
import io.crate.metadata.ReferenceIdent;
import io.crate.metadata.RelationName;
import io.crate.metadata.RowGranularity;
import io.crate.metadata.Schemas;
import io.crate.metadata.table.Operation;
import io.crate.sql.tree.ColumnPolicy;
import org.elasticsearch.test.ESTestCase;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;

public class DocTableInfoTest extends ESTestCase {

    @Test
    public void testGetColumnInfo() throws Exception {
        RelationName relationName = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");

        DocTableInfo info = new DocTableInfo(
            relationName,
            ImmutableList.of(
                new Reference(
                    new ReferenceIdent(relationName, new ColumnIdent("o", ImmutableList.of())),
                    RowGranularity.DOC,
                    DataTypes.UNTYPED_OBJECT,
                    null,
                    null
                )
            ),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            Map.of(),
            Map.of(),
            Map.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            Settings.EMPTY,
            ImmutableList.of(),
            ImmutableList.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );

        Reference foobar = info.getReference(new ColumnIdent("o", ImmutableList.of("foobar")));
        assertNull(foobar);
        DynamicReference reference = info.getDynamic(new ColumnIdent("o", ImmutableList.of("foobar")), false);
        assertNull(reference);
        reference = info.getDynamic(new ColumnIdent("o", ImmutableList.of("foobar")), true);
        assertNotNull(reference);
        assertSame(reference.valueType(), DataTypes.UNDEFINED);
    }

    @Test
    public void testGetColumnInfoStrictParent() throws Exception {
        RelationName dummy = new RelationName(Schemas.DOC_SCHEMA_NAME, "dummy");
        ReferenceIdent foobarIdent = new ReferenceIdent(dummy, new ColumnIdent("foobar"));
        Reference strictParent = new Reference(
            foobarIdent,
            RowGranularity.DOC,
            DataTypes.UNTYPED_OBJECT,
            ColumnPolicy.STRICT,
            Reference.IndexType.NOT_ANALYZED,
            true,
            null,
            null
        );

        Map<ColumnIdent, Reference> references = Map.of(new ColumnIdent("foobar"), strictParent);

        DocTableInfo info = new DocTableInfo(
            dummy,
            ImmutableList.of(strictParent),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            Map.of(),
            references,
            Map.of(),
            ImmutableList.of(),
            ImmutableList.of(),
            null,
            true,
            new String[0],
            new String[0],
            new IndexNameExpressionResolver(),
            5,
            "0",
            Settings.EMPTY,
            ImmutableList.of(),
            ImmutableList.of(),
            ColumnPolicy.DYNAMIC,
            Version.CURRENT,
            null,
            false,
            Operation.ALL
        );


        ColumnIdent columnIdent = new ColumnIdent("foobar", Arrays.asList("foo", "bar"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        columnIdent = new ColumnIdent("foobar", Collections.singletonList("foo"));
        assertNull(info.getReference(columnIdent));
        assertNull(info.getDynamic(columnIdent, false));

        Reference colInfo = info.getReference(new ColumnIdent("foobar"));
        assertNotNull(colInfo);
    }
}
