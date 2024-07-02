
package org.elasticsearch.common.unit;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

public class ByteSizeValueTest {

    @Test
    public void test_can_print_negative_byte_values() throws Exception {
        assertThat(ByteSizeValue.humanReadableBytes(- 283479283)).isEqualTo("-270.3mb");
    }
}


