/*
 * Licensed to CRATE Technology GmbH ("Crate") under one or more contributor
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

package io.crate.analyze;

import com.google.common.base.Optional;
import io.crate.metadata.*;
import io.crate.operator.operator.*;
import io.crate.planner.RowGranularity;
import io.crate.planner.symbol.*;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.*;
import org.apache.lucene.store.RAMDirectory;
import org.apache.lucene.util.Version;
import org.cratedb.DataType;
import org.elasticsearch.common.inject.ModulesBuilder;
import org.elasticsearch.common.lucene.search.MatchNoDocsQuery;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class LuceneQueryBuilderTest {

    LuceneQueryBuilder builder = new LuceneQueryBuilder();
    Functions functions;
    TableIdent characters = new TableIdent(null, "characters");
    Reference name_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "name"), RowGranularity.DOC, DataType.STRING));
    Reference age_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "age"), RowGranularity.DOC, DataType.INTEGER));
    Reference weight_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "weight"), RowGranularity.DOC, DataType.DOUBLE));
    Reference float_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "float_ref"), RowGranularity.DOC, DataType.FLOAT));
    Reference long_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "long_ref"), RowGranularity.DOC, DataType.LONG));
    Reference short_ref = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "short_ref"), RowGranularity.DOC, DataType.SHORT));
    Reference isParanoid = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "isParanoid"), RowGranularity.DOC, DataType.BOOLEAN));
    Reference extrafield = new Reference(new ReferenceInfo(
            new ReferenceIdent(characters, "extrafield"), RowGranularity.DOC, DataType.STRING));

    private IndexSearcher indexSeacher;
    private Sort sort;

    @Before
    public void setUp() throws Exception {
        functions = new ModulesBuilder()
                .add(new OperatorModule())
                .createInjector().getInstance(Functions.class);

        RAMDirectory indexDirectory = new RAMDirectory();
        IndexWriterConfig indexWriterConfig = new IndexWriterConfig(Version.LUCENE_46, null);
        IndexWriter indexWriter = new IndexWriter(indexDirectory, indexWriterConfig);
        SearcherFactory searcherFactory = new SearcherFactory();
        SearcherManager searcherManager = new SearcherManager(indexWriter, true, searcherFactory);

        StringField name = new StringField("name", "", Field.Store.YES);
        IntField age = new IntField("age", 0, Field.Store.YES);
        DoubleField weight = new DoubleField("weight", 0.0, Field.Store.YES);
        FloatField floatField = new FloatField("float_ref", 0.0f, Field.Store.YES);
        LongField longField = new LongField("long_ref", 0, Field.Store.YES);
        // lucene has no shortField -> int is used instead
        IntField shortField = new IntField("short_ref", 0, Field.Store.YES);
        StringField isParanoid = new StringField("isParanoid", "", Field.Store.YES);



        name.setStringValue("Marvin");
        age.setIntValue(84);
        weight.setDoubleValue(492.0);
        floatField.setFloatValue(22.2f);
        longField.setLongValue(8L);
        shortField.setIntValue(2);
        isParanoid.setStringValue("T");

        Document doc = new Document();
        doc.add(name);
        doc.add(age);
        doc.add(weight);
        doc.add(floatField);
        doc.add(longField);
        doc.add(shortField);
        doc.add(isParanoid);
        indexWriter.addDocument(doc);

        name.setStringValue("Trillian");
        age.setIntValue(40);
        weight.setDoubleValue(54.2);
        floatField.setFloatValue(42.2f);
        longField.setLongValue(16L);
        shortField.setIntValue(3);
        isParanoid.setStringValue("F");

        doc = new Document();
        doc.add(name);
        doc.add(age);
        doc.add(weight);
        doc.add(floatField);
        doc.add(longField);
        doc.add(shortField);
        doc.add(isParanoid);
        indexWriter.addDocument(doc);

        name.setStringValue("Arthur");
        age.setIntValue(43);
        weight.setDoubleValue(84.1);
        floatField.setFloatValue(12.2f);
        longField.setLongValue(300L);
        shortField.setIntValue(4);
        isParanoid.setStringValue("F");

        doc = new Document();
        doc.add(name);
        doc.add(age);
        doc.add(weight);
        doc.add(floatField);
        doc.add(longField);
        doc.add(shortField);
        doc.add(isParanoid);
        indexWriter.addDocument(doc);

        StringField extrafield = new StringField("extrafield", "", Field.Store.YES);
        doc = new Document();
        doc.add(extrafield);
        indexWriter.addDocument(doc);

        searcherManager.maybeRefresh();
        sort = new Sort(new SortField("name", SortField.Type.STRING));
        indexSeacher = searcherManager.acquire();
    }

    private List<DataType> typeX2(DataType type) {
        return Arrays.asList(type, type);
    }

    @Test
    public void testEmptyClause() throws Exception {
        Query query = builder.convert(Optional.<Symbol>absent());
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    @Test
    public void testWhereTrue() throws Exception {
        Query query = builder.convert(new BooleanLiteral(true));
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    @Test
    public void testWhereFalse() throws Exception {
        Query query = builder.convert(new BooleanLiteral(false));
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    @Test
    public void testWhereWithAndNested() throws Exception {
        // where name = marvin and age = 84 and longField = 8

        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        FunctionImplementation eqAgeImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.INTEGER)));
        FunctionImplementation eqLongImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.LONG)));
        FunctionImplementation andImpl = functions.get(new FunctionIdent(AndOperator.NAME, typeX2(DataType.BOOLEAN)));

        Function eqName = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Function eqAge = new Function(eqAgeImpl.info(), Arrays.<Symbol>asList(age_ref, new IntegerLiteral(84)));
        Function eqLong = new Function(eqLongImpl.info(), Arrays.<Symbol>asList(long_ref, new LongLiteral(8L)));

        Function rightAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqAge, eqLong));
        Function leftAnd = new Function(andImpl.info(), Arrays.<Symbol>asList(eqName, rightAnd));

        Query query = builder.convert(leftAnd);
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));
    }

    @Test
    public void testWhereWithOr() throws Exception {
        // where name = marvin and age = 84 and longField = 8

        FunctionImplementation eqStringImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        FunctionImplementation orImpl = functions.get(new FunctionIdent(OrOperator.NAME, typeX2(DataType.BOOLEAN)));

        Function eqMarvin = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Function eqTrillian = new Function(eqStringImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Trillian")));

        Function whereClause = new Function(orImpl.info(), Arrays.<Symbol>asList(eqMarvin, eqTrillian));

        Query query = builder.convert(whereClause);
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(2));
    }

    @Test
    public void testWhereReferenceEqStringLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(TermQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));
    }

    @Test
    public void testWhereReferenceNotEqStringLiteral() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(NotEqOperator.NAME, typeX2(DataType.STRING)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(name_ref, new StringLiteral("Marvin")));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(BooleanQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(3));
    }

    @Test
    public void testWhereReferenceEqIntegerLiteral() throws Exception {
        FunctionImplementation eqImpl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.INTEGER)));
        Function whereClause = new Function(eqImpl.info(), Arrays.<Symbol>asList(age_ref, new IntegerLiteral(40)));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(NumericRangeQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));
    }

    @Test
    public void testWhereReferenceLtDoubleLiteral() throws Exception {
        FunctionImplementation ltImpl = functions.get(new FunctionIdent(LtOperator.NAME, typeX2(DataType.DOUBLE)));
        Function whereClause = new Function(ltImpl.info(), Arrays.<Symbol>asList(weight_ref, new DoubleLiteral(54.3)));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(NumericRangeQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));
    }

    @Test
    public void testWhereReferenceLteFloatLiteral() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LteOperator.NAME, typeX2(DataType.FLOAT)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(float_ref, new FloatLiteral(42.1)));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(NumericRangeQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(2));
    }

    @Test
    public void testWhereReferenceGtLong() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(GtOperator.NAME, typeX2(DataType.LONG)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(long_ref, new LongLiteral(8L)));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(NumericRangeQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(2));
    }
      
    @Test
    public void testWhereReferenceEqShort() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(DataType.SHORT)));
        Function whereClause = new Function(impl.info(), Arrays.<Symbol>asList(short_ref, Literal.forType(DataType.SHORT, (short)2)));
        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(NumericRangeQuery.class));
        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));
    }

    @Test
    public void testWhereReferenceEqBoolean() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(EqOperator.NAME, typeX2(isParanoid.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(isParanoid, Literal.forType(isParanoid.valueType(), true)));

        Query query = builder.convert(whereClause);
        assertThat(query, instanceOf(TermQuery.class));

        TopFieldDocs search = indexSeacher.search(query, 5, sort);
        assertThat(search.totalHits, is(1));

    }

    @Test
    public void testWhereReferenceLikeString() throws Exception {
        FunctionImplementation impl = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));
        Function whereClause = new Function(impl.info(),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("%thu%")));

        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(WildcardQuery.class));
        TopFieldDocs docs = indexSeacher.search(query, 5, sort);
        assertThat(docs.totalHits, is(1));
    }

    @Test
    public void testWhereNotReferenceLikeString() throws Exception {
        FunctionImplementation notOp = functions.get(new FunctionIdent(NotOperator.NAME, Arrays.asList(DataType.BOOLEAN)));
        FunctionImplementation likeOp = functions.get(new FunctionIdent(LikeOperator.NAME, typeX2(name_ref.valueType())));

        Function likeClause = new Function(likeOp.info(),
                Arrays.<Symbol>asList(name_ref, new StringLiteral("%thu%")));
        Function whereClause = new Function(notOp.info(), Arrays.<Symbol>asList(likeClause));

        Query query = builder.convert(whereClause);

        assertThat(query, instanceOf(BooleanQuery.class));
        TopFieldDocs docs = indexSeacher.search(query, 5, sort);
        assertThat(docs.totalHits, is(3));
    }

    @Test
    public void testWhereReferenceIsNull() throws Exception {
        FunctionImplementation isNullImpl = functions.get(
                new FunctionIdent(IsNullOperator.NAME, Arrays.asList(extrafield.valueType())));

        Function isNull = new Function(isNullImpl.info(), Arrays.<Symbol>asList(extrafield));
        Query query = builder.convert(isNull);

        TopFieldDocs docs = indexSeacher.search(query, 5, sort);
        assertThat(docs.totalHits, is(3));


        isNull = new Function(isNullImpl.info(), Arrays.<Symbol>asList(name_ref));
        query = builder.convert(isNull);
        docs = indexSeacher.search(query, 5, sort);
        assertThat(docs.totalHits, is(1));
    }
}
