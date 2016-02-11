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

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.util.HashSet;
import java.util.Set;

/**
 *
 */
class TypesFinder {
    private final Client client;

    @Inject
    TypesFinder(Client client) {
        this.client = client;
    }

    Set<String> getTypes(String system) {
        Set<String> types = new HashSet<>();

        SearchResponse searchResponse = client.prepareSearch(system)
                .setTypes("Type")
                .setQuery(QueryBuilders.matchAllQuery())
                .setSize(2147483647)
                .execute()
                .actionGet();

        for (SearchHit hit : searchResponse.getHits().hits()) {
            String typeName = (String) hit.getSource().get("typeName");

            SearchResponse childrenResponse = client.prepareSearch(system)
                    .setTypes("Type")
                    .setSize(0)
                    .setQuery(QueryBuilders.termQuery("parentTypes.raw", typeName))
                    .execute()
                    .actionGet();

            if (childrenResponse.getHits().getTotalHits() == 0) {
                types.add(typeName);
            }
        }

        return types;
    }
}
