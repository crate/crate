/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.gcs;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;

import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.IntegTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sun.net.httpserver.HttpServer;

@IntegTestCase.ClusterScope(scope = IntegTestCase.Scope.TEST)
public class GCSSnapshotIntegrationTest extends IntegTestCase {

    // This is an arbitrary rsa key in pkcs8 format for testing
    // which is not used in the real world. It can be generated with
    // the command `openssl genrsa -out keypair.pem 2048`
    static final String PKCS8_PRIVATE_KEY = """
        -----BEGIN PRIVATE KEY-----
        MIIEvgIBADANBgkqhkiG9w0BAQEFAASCBKgwggSkAgEAAoIBAQC9iMiyCPtIJz8a
        tYrijSfRaCAmGAJQ2SZYrOQ7OSta+fbsvgtmYkKpy0HS1RuLpS5j1pTKqwWtxkJp
        f110TuS3D6JNhrEWDZkQDoaaFE9JWXQo9lROEtSzgcpk74i6Fz5yBaehgMV4pHQP
        9SqZN+PU/m/rGInSgFy6m2BRznyYKy1fXYaIF351rF/AtAiimndDbS80e4risQN7
        WjgKDTd1UFfg4kEC0TQeaqif4N/J2kTQq6Fvm10a6TwwgL4wuuJ9b8AZmbWLH2Ck
        t3kC3E8ZyLiC2bOCRA7DEMMndK72Bms1JreAGVjJVqa37b3clFOdKdrQKGzLzLL2
        7eX1v4uFAgMBAAECggEBAJlBAQb0PDsbgOsX4DVP7eJlT5l90GGPNHJ/WgyJLYVi
        mUbUZGNlEII61/6iUqOX7OrNl4JIx068APdNBUQGhul+ur31Kzupwxo4pJ3xziqB
        Kmv0wjZfA54iVIVJKkVOhi+sYt80QHhMgYxlsQwzJQYUtmpibQ7IvDIncLq1PAnN
        ejbEVMBvRquUOxL6ctO6sBtrsUUWhlXqxpsDJMUoIFzRrhRuOQP5YWCi4OpRbvII
        qqUnl+lPofsHRjvt1sKR5RxiMklBggUAcMReNapViMz7Bz3hRSLjDKjE2q8aaDas
        ezeZqYI0mbQSSjoBXaHTHP6I4TOOMuoCvkkuhtpF1AECgYEA8QoOCGbXWArjrXWR
        gAmXIlRxVDZHmyMAjArkEKwCmgJu1G1nxN7jY+x93qGUuikO3xlhH/Wo8kBn12jc
        XyrEOo5qIOuTqSBwuA+INLbwQRyeX3iP0CALyKNZEAlAa1uS22XUU4m7x7hqMq8L
        yqi7g/QiQG1gBWQcJzxUeKmD1ScCgYEAyUxZ8vn3JkmSZ0hnJKobbd7FsBc1bV6t
        2veGipLZRjyIateEASdlSPVOLD+ZoPlCmg4VCDJV6xx4a+Af89BHeBi5zVi/z3oh
        GqV/rpzfcMJnKIiMCbOmHKMIsBWZj+NRT6KzAOP6fqDJ1V3ZzBN087+52IxLAC1O
        CEU0P38vvXMCgYAnohecmgxelavKIcLC4tDO/EOGLUao46B7Zm8Jrr7ew/elRjgB
        zwRkscYgjUD/OzEOzgWCU8pryttIOB3EKCwL1M7uis3EyWi/Ww5yXII0spf36sL6
        3coSO4mxcVP+Uxhaquu2sLcHp/MOUmoF8KikkcfwAAwB1uwqJ2lcTcM3kQKBgBik
        jzJupXH7eb/JHk9fv8HgjsTy4miEObZfrQnT1mOBz5V80r0tbHnVBf/mvVD2ks+3
        P53kQ55nutpB8sdvTQCHzl80KS8mHV1cu1fN/pCYS/arWLFrW7+PueWMj2MNCgw8
        t7s5LZZI6syDE8Gm9B9O7lpzOk9IPJBIoI/Ray+/AoGBANYLcri2nHdlgJJtibko
        GlTJmBlNP/0gJe5FQYB5tnDO9yqDXXCAhMNrVQc3WmhVvX7UtaUL9zaq3cYO9PT5
        UEij2bajHWK+vdYDdj/mvnPBrIZshPWau2h1lNGV/FjsUvxi2K4rvjXIpKBnom1L
        aXHxlsqXzR0B4pnnvS7L6VI9
        -----END PRIVATE KEY-----
        """;

    private static final String BUCKET_NAME = "bucket";

    private HttpServer oauthServer;
    private HttpServer gcsServer;
    private GCSHttpHandler gcsHandler;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(GCSRepositoryPlugin.class);
        return plugins;
    }

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        this.oauthServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        this.oauthServer.createContext("/token", new FakeOAuth2HttpHandler());
        this.oauthServer.start();

        this.gcsServer = HttpServer.create(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0), 0);
        this.gcsHandler = new GCSHttpHandler(BUCKET_NAME);
        this.gcsServer.createContext("/", gcsHandler);
        this.gcsServer.start();
    }

    @Override
    @After
    public void tearDown() throws Exception {
        super.tearDown();

        oauthServer.stop(1);
        gcsServer.stop(1);
    }

    @Test
    public void create_restore_drop_snapshot() {
        execute("create table t1 (x int)");
        assertThat(response.rowCount()).isEqualTo(1L);

        int numberOfDocs = randomIntBetween(0, 500);
        Object[][] rows = new Object[numberOfDocs][];
        for (int i = 0; i < numberOfDocs; i++) {
            rows[i] = new Object[]{randomInt()};
        }
        execute("insert into t1 (x) values (?)", rows);
        execute("refresh table t1");

        execute("create repository r1 type gcs with (" +
                "bucket = '" + BUCKET_NAME + "', " +
                "project_id = 'my-project', " +
                "endpoint = '" + gscServerUrl() + "'," +
                "token_uri = '" + oauthServerUrl() + "'," +
                "base_path = 'path', " +
                "private_key_id = 'id123'," +
                "private_key = '" + PKCS8_PRIVATE_KEY + "'," +
                "client_id = 'id123'," +
                "client_email = 'bla@foobar.com')");

        assertThat(response.rowCount()).isEqualTo(1L);

        execute("create snapshot r1.s1 all with (wait_for_completion = true)");

        execute("drop table t1");

        execute("restore snapshot r1.s1 all with (wait_for_completion = true)");
        execute("refresh table t1");

        execute("select count(*) from t1");
        assertThat(response.rows()[0][0]).isEqualTo((long) numberOfDocs);

        execute("drop snapshot r1.s1");
        gcsHandler.blobs().keySet().forEach(x -> assertThat(x).doesNotEndWith("dat"));
    }

    private String oauthServerUrl() {
        InetSocketAddress address = oauthServer.getAddress();
        return "http://" + address.getAddress().getCanonicalHostName() + ":" + address.getPort() + "/token";
    }

    private String gscServerUrl() {
        InetSocketAddress address = gcsServer.getAddress();
        return "http://" + address.getAddress().getCanonicalHostName() + ":" + address.getPort();
    }

}
