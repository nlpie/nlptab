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

package edu.umn.nlptab.analysis;

import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.io.Closeable;
import java.io.IOException;

/**
 *
 */
class FsDataSource implements Closeable {

    private static final ESLogger LOGGER = Loggers.getLogger(FsDataSource.class);

    private final Client client;

    private final String documentIdentifier;

    private final UnitOfAnalysis unitOfAnalysis;

    @Nullable
    private SearchResponse response;

    @Nullable
    private SearchHit[] hits;

    FsDataSource(Client client, String documentIdentifier, UnitOfAnalysis unitOfAnalysis) {
        this.client = client;
        this.documentIdentifier = documentIdentifier;
        this.unitOfAnalysis = unitOfAnalysis;
    }

    int advance() {
        if (response == null) {
            response = client.prepareSearch(unitOfAnalysis.getSystemIndex())
                    .setQuery(unitOfAnalysis.queryInDocument(documentIdentifier))
                    .setScroll(TimeValue.timeValueMinutes(2))
                    .execute().actionGet();
        }

        response = client.prepareSearchScroll(response.getScrollId())
                .setScroll(TimeValue.timeValueMinutes(2))
                .execute().actionGet();
        hits = response.getHits().hits();
        return hits.length;
    }

    SearchHit getSearchHit(int index) {
        return hits[index];
    }

    @Override
    public void close() throws IOException {
        if (response != null) {
            ClearScrollResponse clearScrollResponse = client.prepareClearScroll()
                    .addScrollId(response.getScrollId())
                    .execute()
                    .actionGet();

            if (!clearScrollResponse.isSucceeded()) {
                LOGGER.debug("Failed to clear scroll: {}", clearScrollResponse.status());
            }
        }
    }
}
