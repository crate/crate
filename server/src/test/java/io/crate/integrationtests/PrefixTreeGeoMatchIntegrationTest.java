package io.crate.integrationtests;

import org.elasticsearch.index.mapper.GeoShapeFieldMapper;

public class PrefixTreeGeoMatchIntegrationTest extends BaseGeoMatchIntegrationTest {

    @Override
    String indexType() {
        return GeoShapeFieldMapper.Names.TREE_GEOHASH;
    }
}
