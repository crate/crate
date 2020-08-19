package io.crate.cluster.decommission;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractWireSerializingTestCase;

public class DecommissionNodeRequestTest extends AbstractWireSerializingTestCase<DecommissionNodeRequest> {

    @Override
    protected DecommissionNodeRequest createTestInstance() {
        return new DecommissionNodeRequest();
    }

    @Override
    protected Reader<DecommissionNodeRequest> instanceReader() {
        return DecommissionNodeRequest::new;
    }
}
