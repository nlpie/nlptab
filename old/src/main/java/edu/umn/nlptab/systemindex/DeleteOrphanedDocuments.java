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

package edu.umn.nlptab.systemindex;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.delete.DeleteRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

/**
 *
 */
public class DeleteOrphanedDocuments implements Runnable {

    private final Client client;

    private String searchIndex;

    @Inject
    DeleteOrphanedDocuments(Client client) {
        this.client = client;
    }

    public DeleteOrphanedDocuments inSearchIndex(String searchIndex) {
        this.searchIndex = searchIndex;
        return this;
    }

    @Override
    public void run() {
        SearchResponse searchResponse = client.prepareSearch(searchIndex)
                .setTypes("Document")
                .setQuery(QueryBuilders.boolQuery()
                        .mustNot(QueryBuilders.hasChildQuery("DocumentInSystem", QueryBuilders.matchAllQuery())))
                .setSize(Integer.MAX_VALUE)
                .get();

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();
        for (SearchHit searchHit : searchResponse.getHits().hits()) {
            DeleteRequestBuilder delete = client.prepareDelete(searchIndex, "Document", searchHit.getId());
            bulkRequestBuilder.add(delete);
        }

        if (bulkRequestBuilder.numberOfActions() > 0) {
            bulkRequestBuilder.get();
        }
    }
}
