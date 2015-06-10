/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.cache.fixedbitset;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queries.TermFilter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.RAMDirectory;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.XConstantScoreQuery;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.equalTo;

/**
 */
public class FixedBitSetFilterCacheTest extends ElasticsearchTestCase {

    private ThreadPool threadPool;

    @Before
    public void init() {
        threadPool = new ThreadPool("test");
    }

    @After
    public void cleanup() throws InterruptedException {
        terminate(threadPool);
    }

    @Test
    public void testInvalidateEntries() throws Exception {
        IndexWriter writer = new IndexWriter(
                new RAMDirectory(),
                new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)).setMergePolicy(new LogByteSizeMergePolicy())
        );
        Document document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        IndexReader reader = DirectoryReader.open(writer, false);
        IndexSearcher searcher = new IndexSearcher(reader);

        FixedBitSetFilterCache cache = new FixedBitSetFilterCache(new Index("test"), ImmutableSettings.EMPTY, threadPool);
        FixedBitSetFilter filter = cache.getFixedBitSetFilter(new TermFilter(new Term("field", "value")));
        TopDocs docs = searcher.search(new XConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        docs = searcher.search(new XConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));
        // There are 3 segments
        assertThat(cache.getLoadedFilters().size(), equalTo(3L));

        writer.forceMerge(1);
        reader.close();
        reader = DirectoryReader.open(writer, false);
        searcher = new IndexSearcher(reader);

        docs = searcher.search(new XConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        docs = searcher.search(new XConstantScoreQuery(filter), 1);
        assertThat(docs.totalHits, equalTo(3));
        // Only one segment now, so the size must be 1
        assertThat(cache.getLoadedFilters().size(), equalTo(1L));

        reader.close();
        writer.close();
        // There is no reference from readers and writer to any segment in the test index, so the size in the fbs cache must be 0
        assertThat(cache.getLoadedFilters().size(), equalTo(0L));
    }

    @Test
    public void testCachedEntries() throws Exception {
        IndexWriter writer = new IndexWriter(
                new RAMDirectory(),
                new IndexWriterConfig(Lucene.VERSION, new StandardAnalyzer(Lucene.VERSION)).setMergePolicy(new LogByteSizeMergePolicy())
        );
        Document document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        document = new Document();
        document.add(new StringField("field", "value", Field.Store.NO));
        writer.addDocument(document);
        writer.commit();

        IndexReader reader = DirectoryReader.open(writer, false);
        IndexSearcher searcher = new IndexSearcher(reader);

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("index.cache.fixedbitset.size", 1024L * 1024L)
                .put("index.cache.fixedbitset.expire", TimeValue.timeValueHours(1))
                .build();
        FixedBitSetFilterCache cache = new FixedBitSetFilterCache(new Index("test"), settings, threadPool);
        TermFilter termFilter = new TermFilter(new Term("field", "value"));
        FixedBitSetFilter filter = cache.getFixedBitSetFilter(termFilter);
        XConstantScoreQuery query = new XConstantScoreQuery(filter);

        TopDocs docs = searcher.search(query, 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        docs = searcher.search(query, 1);
        assertThat(docs.totalHits, equalTo(3));
        // There are 3 segments
        assertThat(cache.getLoadedFilters().size(), equalTo(3L));

        writer.forceMerge(1);
        reader.close();
        reader = DirectoryReader.open(writer, false);
        searcher = new IndexSearcher(reader);

        docs = searcher.search(query, 1);
        assertThat(docs.totalHits, equalTo(3));

        // now cached
        for (int i = 0; i < 100; i++) {
            docs = searcher.search(query, 1);
            assertThat(docs.totalHits, equalTo(3));
            // Only one segment now, so the size must be 1
            assertThat(cache.getLoadedFilters().size(), equalTo(1L));
        }

        reader.close();
        writer.close();
        // There is no reference from readers and writer to any segment in the test index, so the size in the fbs cache must be 0
        assertThat(cache.getLoadedFilters().size(), equalTo(0L));
    }
}
