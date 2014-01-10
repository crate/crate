package io.crate.udc;


import junit.framework.TestCase;
import org.hyperic.sigar.OperatingSystem;
import org.junit.Test;

public class UdcDataTest extends TestCase {

    @Test
    public void testSigarData() {
        System.out.println(OperatingSystem.getInstance().toMap());
    }
}
