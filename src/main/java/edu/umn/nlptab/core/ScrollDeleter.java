/*
 * Copyright (c) 2015 Regents of the University of Minnesota.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.umn.nlptab.core;

import com.google.common.base.Preconditions;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class ScrollDeleter implements Callable<Void> {

    private final Client client;

    @Nullable
    private String[] indexes;

    @Nullable
    private String[] types;

    @Nullable
    private QueryBuilder queryBuilder;

    @Inject
    ScrollDeleter(Client client) {
        this.client = client;
    }

    public ScrollDeleter withIndexes(String... indexes) {
        this.indexes = indexes;
        return this;
    }

    public ScrollDeleter withTypes(String... types) {
        this.types = types;
        return this;
    }

    public ScrollDeleter withQuery(QueryBuilder queryBuilder) {
        this.queryBuilder = queryBuilder;
        return this;
    }

    @Override
    public Void call() throws Exception {
        executeDelete();
        return null;
    }

    public void executeDelete() {
        Preconditions.checkNotNull(indexes);
        Preconditions.checkNotNull(types);
        Preconditions.checkNotNull(queryBuilder);

        SearchResponse searchResponse = client.prepareSearch(indexes)
                .setTypes(types)
                .setScroll(new TimeValue(60, TimeUnit.SECONDS))
                .setSize(500)
                .setQuery(queryBuilder)
                .get();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        while (true) {
            searchResponse = client.prepareSearchScroll(searchResponse.getScrollId())
                    .setScroll(new TimeValue(60, TimeUnit.SECONDS))
                    .get();
            SearchHit[] hits = searchResponse.getHits().hits();
            if (hits.length == 0) {
                break;
            }

            for (SearchHit searchHit : hits) {
                DeleteRequestBuilder delete = client.prepareDelete(searchHit.index(), searchHit.type(),
                        searchHit.id());

                bulkRequestBuilder.add(delete);
            }
        }

        client.prepareClearScroll()
                .addScrollId(searchResponse.getScrollId())
                .get();

        if (bulkRequestBuilder.numberOfActions() > 0) {
            bulkRequestBuilder.get();
        }
    }
}
