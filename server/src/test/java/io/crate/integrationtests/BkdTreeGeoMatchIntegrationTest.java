package io.crate.integrationtests;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;

import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.junit.Test;

import io.crate.exceptions.UnsupportedFeatureException;
import io.crate.testing.UseJdbc;

public class BkdTreeGeoMatchIntegrationTest extends BaseGeoMatchIntegrationTest {

    @Override
    String indexType() {
        return GeoShapeFieldMapper.Names.TREE_BKD;
    }

    @UseJdbc(0)
    @Test
    public void test_geo_match_within_does_not_support_linestring() {
        execute("create table t (s geo_shape index using bkdtree) with (number_of_replicas=0)");
        ensureGreen();
        execute("insert into t (s) values ('POLYGON((13 53, 13 52, 14 52, 14 53, 13 53))')");
        execute("refresh table t");

        List<String> lineStrings = List.of(
            "LINESTRING(13 53, 13 52, 14 52, 14 53, 13 53)",
            "MULTILINESTRING((14.05 53.05, 12.85 53.05, 12.85 51.85, 14.05 51.85), (14.05 51.85, 14.05 53.05))"
        );
        for (String line : lineStrings) {
            assertThatThrownBy(() -> execute("select * from t where match(s, '%s') using within".formatted(line)))
                .isExactlyInstanceOf(UnsupportedFeatureException.class)
                .hasMessage("WITHIN queries with line geometries are not supported");
        }
    }
}
