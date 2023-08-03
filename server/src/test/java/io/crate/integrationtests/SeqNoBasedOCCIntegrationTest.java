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

package io.crate.integrationtests;

import static org.junit.Assert.assertEquals;

import org.elasticsearch.test.IntegTestCase;
import org.junit.Test;

import io.crate.testing.UseRandomizedOptimizerRules;


public class SeqNoBasedOCCIntegrationTest extends IntegTestCase {

    @Test
    @UseRandomizedOptimizerRules(0) // depends on realtime result via primary key lookup
    public void testDeleteWhereSeqNoAndTermThatMatch() throws Exception {
        execute("create table t (x integer primary key, y string) with (number_of_replicas=0)");
        execute("insert into t (x, y) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select _seq_no, _primary_term from t where x = 1");
        assertEquals(1L, response.rowCount());
        Long seqNo = (Long) response.rows()[0][0];
        Long primaryTerm = (Long) response.rows()[0][1];

        execute("delete from t where x = 1 and _seq_no = ? and _primary_term = ?",
            new Object[]{seqNo, primaryTerm});
        assertEquals(1L, response.rowCount());

        execute("select * from t where x = 1");
        assertEquals("Records with x=1 must be deleted", 0, response.rowCount());
    }

    @Test
    public void testDeleteWithSeqNoAndTermThatConflict() throws Exception {
        execute("create table t (x integer primary key, y string)");
        execute("insert into t (x, y) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select _seq_no, _primary_term from t where x = 1");
        assertEquals(1L, response.rowCount());
        Long preUpdateSeqNo = (Long) response.rows()[0][0];
        Long preUpdatePrimaryTerm = (Long) response.rows()[0][1];

        execute("update t set y = ? where x = ?", new Object[]{"ok now panic", 1});
        assertEquals(1L, response.rowCount());
        refresh();

        execute("delete from t where x = 1 and _seq_no = ? and _primary_term = ?",
                new Object[]{preUpdateSeqNo, preUpdatePrimaryTerm});
        assertEquals(0, response.rowCount());
    }

    @Test
    @UseRandomizedOptimizerRules(0) // depends on realtime result via primary key lookup
    public void testUpdateWhereSeqNoAndPrimaryTermWithPrimaryKey() throws Exception {
        execute("create table t (x integer primary key, y string)");
        execute("insert into t (x, y) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select _seq_no, _primary_term from t where x = 1");
        assertEquals(1L, response.rowCount());
        Long preUpdateSeqNo = (Long) response.rows()[0][0];
        Long preUpdatePrimaryTerm = (Long) response.rows()[0][1];

        execute("update t set y = ? where x = ? and _seq_no = ? and _primary_term = ?",
                new Object[]{"ok now panic", 1, preUpdateSeqNo, preUpdatePrimaryTerm});
        assertEquals(1L, response.rowCount());

        execute("select y from t where x = 1");
        assertEquals(1L, response.rowCount());
        assertEquals("The y column of record with x=1 must be updated.", "ok now panic", response.rows()[0][0]);
    }

    @Test
    public void testUpdateWhereSeqNoAndPrimaryTermWithConflict() throws Exception {
        execute("create table t (x integer primary key, y string)");
        ensureYellow();

        execute("insert into t (x, y) values (?, ?)", new Object[]{1, "don't panic"});
        refresh();

        execute("select _seq_no, _primary_term from t where x = 1");
        assertEquals(1L, response.rowCount());
        Long preUpdateSeqNo = (Long) response.rows()[0][0];
        Long preUpdatePrimaryTerm = (Long) response.rows()[0][1];

        execute("update t set y = ? where x = ? and _seq_no = ? and _primary_term = ?",
                new Object[]{"hopefully not updated", 1, preUpdateSeqNo + 4, preUpdatePrimaryTerm});
        assertEquals(0L, response.rowCount());

        // Validate that the row is really NOT updated
        refresh();
        execute("select y from t where x = 1");
        assertEquals(1L, response.rowCount());
        assertEquals("don't panic", response.rows()[0][0]);
    }
}
