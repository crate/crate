/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.monitor;

import io.crate.test.integration.CrateUnitTest;
import org.apache.lucene.util.LuceneTestCase;
import org.hamcrest.core.AnyOf;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.core.AnyOf.anyOf;
import static org.hamcrest.core.Is.is;

public class SysInfoUtilTest extends CrateUnitTest {

    private static final AnyOf<String> X86_64 = anyOf(is("x86_64"), is("amd64"), is("x64"));

    @Test
    public void testParseKeyValue() {
        assertThat(SysInfoUtil.parseKeyValue("KEY=\"val\"ue\""), is(new String[]{"KEY", "val\"ue"}));
        assertThat(SysInfoUtil.parseKeyValue("KEY=\"value\""), is(new String[]{"KEY", "value"}));
        assertThat(SysInfoUtil.parseKeyValue("KEY=value"), is(new String[]{"KEY", "value"}));
        assertThat(SysInfoUtil.parseKeyValue("KEY="), is(new String[]{"KEY", ""}));
        assertThat(SysInfoUtil.parseKeyValue("KEY"), is(new String[]{"KEY", ""}));
        assertThat(SysInfoUtil.parseKeyValue(""), is(new String[]{"", ""}));
    }

    @Test
    public void testWindows() {
        SysInfoUtil util = new SysInfoUtil("Windows 10", "10.0", "x86_64");
        assertThat(util.info().arch(), is("x86_64"));
        assertThat(util.info().description(), is("Microsoft Windows 10"));
        assertThat(util.info().machine(), X86_64);
        assertThat(util.info().name(), is("Win32"));
        assertThat(util.info().patchLevel(), is(""));
        assertThat(util.info().vendor(), is("Microsoft"));
        assertThat(util.info().vendorCodeName(), is(""));
        assertThat(util.info().vendorName(), is("Windows 10"));
        assertThat(util.info().vendorVersion(), is("10"));
        assertThat(util.info().version(), is("10.0"));
    }

    @Test
    public void testMacOS() {
        SysInfoUtil util = new SysInfoUtil("Mac OS", "10.12.6", "x86_64");
        assertThat(util.info().arch(), is("x86_64"));
        assertThat(util.info().description(), is("Mac OS X (Sierra)"));
        assertThat(util.info().machine(), X86_64);
        assertThat(util.info().name(), is("MacOSX"));
        assertThat(util.info().patchLevel(), is(""));
        assertThat(util.info().vendor(), is("Apple"));
        assertThat(util.info().vendorCodeName(), is("Sierra"));
        assertThat(util.info().vendorName(), is("Mac OS X"));
        assertThat(util.info().vendorVersion(), is("10.12"));
        assertThat(util.info().version(), is("10.12.6"));
    }

    @Test
    public void testDarwin() {
        SysInfoUtil util = new SysInfoUtil("Darwin", "16.6.0", "x86_64");
        assertThat(util.info().arch(), is("x86_64"));
        assertThat(util.info().description(), is("Mac OS X (Sierra)"));
        assertThat(util.info().machine(), X86_64);
        assertThat(util.info().name(), is("MacOSX"));
        assertThat(util.info().patchLevel(), is(""));
        assertThat(util.info().vendor(), is("Apple"));
        assertThat(util.info().vendorCodeName(), is("Sierra"));
        assertThat(util.info().vendorName(), is("Mac OS X"));
        assertThat(util.info().vendorVersion(), is("16.6"));
        assertThat(util.info().version(), is("16.6.0"));
    }

    @Test
    public void testParseRedHatVendor() throws IOException {
        SysInfoUtil util = new SysInfoUtil("Linux", "3.10.0-693.2.2.el7.x86_64", "x86_64");

        Path centosRelease = LuceneTestCase.createTempFile();
        List<String> lines = Collections.singletonList("CentOS Linux release 7.4.1708 (Core)\n");
        Files.write(centosRelease, lines, StandardCharsets.UTF_8);
        util.parseRedHatVendor(centosRelease.toFile());
        assertThat(util.info().vendorVersion(), is("7.4.1708"));
        assertThat(util.info().vendor(), is("CentOS"));
        assertThat(util.info().vendorCodeName(), is("Core"));

        Path rhRelease = LuceneTestCase.createTempFile();
        lines = Collections.singletonList("Red Hat Enterprise Linux Server release 6.7 (Santiago)\n");
        Files.write(rhRelease, lines, StandardCharsets.UTF_8);
        util.parseRedHatVendor(rhRelease.toFile());
        assertThat(util.info().vendorVersion(), is("Enterprise Linux 6"));
        assertThat(util.info().vendorCodeName(), is("Santiago"));
    }

    @Test
    public void testFailedSysCall() {
        // by default sys call filter is enabled, so any Runtime.getRuntime().exec(...) will fail
        List<String> result = SysInfoUtil.sysCall(new String[]{"undefined"}, "default");
        assertThat(result.get(0), is("default"));
    }
}
