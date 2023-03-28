package io.crate.execution.ddl.tables;

import static io.crate.analyze.AnalyzedColumnDefinition.typeNameForESMapping;
import static org.assertj.core.api.Assertions.assertThat;

import com.carrotsearch.hppc.IntArrayList;
import io.crate.metadata.*;
import io.crate.sql.tree.ColumnPolicy;
import io.crate.sql.tree.QualifiedName;
import io.crate.types.DataTypes;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class CreateTableRequestTest {

    @Test
    public void testStreaming() throws Exception {
        RelationName rel = RelationName.of(QualifiedName.of("t1"), Schemas.DOC_SCHEMA_NAME);

        Reference ref1 = new SimpleReference(
            new ReferenceIdent(rel, "part_col_1"),
            RowGranularity.DOC,
            DataTypes.STRING,
            1,
            null
        );
        Reference ref2 = new SimpleReference(
            new ReferenceIdent(rel, "part_col_2"),
            RowGranularity.DOC,
            DataTypes.INTEGER,
            2,
            null
        );
        Reference ref3 = new SimpleReference(
            new ReferenceIdent(rel, "just_col"),
            RowGranularity.DOC,
            DataTypes.BYTE,
            3,
            null
        );
        Reference ref4 = new SimpleReference(
            new ReferenceIdent(rel, "some_routing_col"),
            RowGranularity.DOC,
            DataTypes.LONG,
            4,
            null
        );
        List<Reference> refs = List.of(ref1, ref2, ref3, ref4);
        AddColumnRequest addColumnRequest = new AddColumnRequest(rel, refs, Map.of("check1", "just_col > 0"), IntArrayList.from(3));
        List<String> partCol1 = List.of("part_col_1", typeNameForESMapping(DataTypes.STRING, "english", false));
        List<String> partCol2 = List.of("part_col_2", typeNameForESMapping(DataTypes.INTEGER, null, false));
        List<List<String>> partitionedBy = List.of(partCol1, partCol2);

        CreateTableRequest request = new CreateTableRequest(
            addColumnRequest,
            Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_3_0).build(),
            "some_routing_col",
            ColumnPolicy.STRICT.lowerCaseName(),
            partitionedBy,
            Map.of("fulltext_index_name", Map.of())
        );

        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        CreateTableRequest fromStream = new CreateTableRequest(out.bytes().streamInput());

        assertThat(fromStream.addColumnRequest().relationName()).isEqualTo(rel);
        assertThat(fromStream.addColumnRequest().references()).containsExactlyElementsOf(refs);
        assertThat(fromStream.addColumnRequest().checkConstraints()).isEqualTo(Map.of("check1", "just_col > 0"));
        assertThat(fromStream.addColumnRequest().pKeyIndices().size()).isEqualTo(1);
        assertThat(fromStream.addColumnRequest().pKeyIndices().get(0)).isEqualTo(3);

        assertThat(fromStream.settings()).isEqualTo(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_5_3_0).build());
        assertThat(fromStream.routingColumn()).isEqualTo("some_routing_col");
        assertThat(fromStream.tableColumnPolicy()).isEqualTo(ColumnPolicy.STRICT.lowerCaseName());
        assertThat(fromStream.partitionedBy()).containsExactlyElementsOf(partitionedBy);
        assertThat(fromStream.indicesMap()).isEqualTo(Map.of("fulltext_index_name", Map.of()));
    }

}
