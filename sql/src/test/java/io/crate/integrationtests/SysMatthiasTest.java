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

package io.crate.integrationtests;

import io.crate.testing.TestingHelpers;
import org.junit.Test;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.is;

public class SysMatthiasTest extends SQLTransportIntegrationTest {

    @Test
    public void testSelectSysMatthias() throws Exception {
        execute("select * from sys.matthias");
        assertThat(response.cols(), arrayContaining("favorite_programming_language", "\"from\"", "gifs", "message", "name", "stats", "title", "to"));
        assertThat(response.rowCount(), is(1L));
        assertThat(TestingHelpers.printedTable(response.rows()), is(
                "Erlang| " +
                "1380610800000| " +
                "[http://tclhost.com/6E71Pl6.gif, " +
                    "http://33.media.tumblr.com/e8091211fe0e60230acd9e1a1e1e2ec9/tumblr_inline_ny8tujouJG1raprkq_500.gif, " +
                    "https://media.giphy.com/media/10QbzBy3KKAw36/giphy.gif, http://www.reactiongifs.com/r/spg.gif, " +
                    "http://www.reactiongifs.com/wp-content/uploads/2013/01/lol-bye.gif, " +
                    "http://www.reactiongifs.com/r/best-gif-ever1.gif]| " +
                "It's been an honour, a pleasure!| " +
                "Matthias Wahl| " +
                "{" +
                    "commits=890, " +
                    "hearts_broken=Infinity, " +
                    "lines_added=164181, " +
                    "lines_removed=82590}| " +
                "CGO (Chief GIF Officer)| " +
                "1450458000000\n"));
    }

    @Test
    public void testSelectStats() throws Exception {
        execute("select stats, stats['commits'], stats['hearts_broken'], stats['lines_added'], stats['lines_removed'] from sys.matthias");
        assertThat(TestingHelpers.printedTable(response.rows()), is("{commits=890, hearts_broken=Infinity, lines_added=164181, lines_removed=82590}| " +
                                                                    "890| " +
                                                                    "Infinity| " +
                                                                    "164181| " +
                                                                    "82590\n"));
    }
}
