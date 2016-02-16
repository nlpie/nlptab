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
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.util.Map;

/**
 *
 */
class ClosestFsFinder {

    private final Client client;

    private UnitOfAnalysis target;

    private Map<String, Object> featureStructure;

    private String documentId;

    @Inject
    ClosestFsFinder(Client client) {
        this.client = client;
    }

    ClosestFsFinder withTarget(UnitOfAnalysis target) {
        this.target = target;
        return this;
    }

    ClosestFsFinder withFeatureStructure(Map<String, Object> featureStructure) {
        this.featureStructure = featureStructure;
        return this;
    }

    ClosestFsFinder withDocumentId(String documentId) {
        this.documentId = documentId;
        return this;
    }

    @Nullable
    String getClosestId(int limit) {
        @SuppressWarnings("unchecked")
        Map<String, Object> primaryLocation = (Map<String, Object>) featureStructure.get("primaryLocation");

        BoolQueryBuilder boolQueryBuilder = target.queryInDocument(documentId);

        if (primaryLocation == null || !primaryLocation.containsKey("begin") || !primaryLocation.containsKey("end")) {
            return null;
        }

        SearchResponse searchResponse;
        if (limit != 0) {
            FunctionScoreQueryBuilder query = new FunctionScoreQueryBuilder(boolQueryBuilder)
                    .add(ScoreFunctionBuilders.scriptFunction(new Script("annotationDistance", ScriptService.ScriptType.INLINE, "native", primaryLocation)));
            float minScore = 1.0f / (limit + 1.0f);
            searchResponse = client.prepareSearch(target.getSystemIndex())
                    .setQuery(query)
                    .setMinScore(minScore)
                    .execute().actionGet();
        } else {
            searchResponse = client.prepareSearch(target.getSystemIndex())
                    .setQuery(boolQueryBuilder.must(QueryBuilders.termQuery("primaryLocation.begin", primaryLocation.get("begin")))
                            .must(QueryBuilders.termQuery("primaryLocation.end", primaryLocation.get("end"))))
                    .get();
        }


        SearchHit[] hits = searchResponse.getHits().hits();
        if (hits.length > 0) {
            return hits[0].getId();
        }

        return null;
    }
}
