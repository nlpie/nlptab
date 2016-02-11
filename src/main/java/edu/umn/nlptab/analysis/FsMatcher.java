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

import com.google.common.base.Preconditions;
import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Searches matches for a feature structure in one system to a feature structure in another system.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class FsMatcher {
    private static final int SIZE = 50;
    /**
     * The ElasticSearch client to use.
     */
    private final Client client;

    /**
     * The Feature Structure to find a match for.
     */
    @Nullable
    private Map<String, Object> featureStructure;

    @Nullable
    private AnalysisConfig analysisConfig;

    @Nullable
    private UnitOfAnalysis target;

    @Nullable
    private Collection<FeatureValueTester> featureStructureTesters;

    @Nullable
    private BoolQueryBuilder boolQueryBuilder;

    private long totalHits;

    @Nullable
    private SearchHit[] hits;

    private int from;

    @Inject
    FsMatcher(Client client) {
        this.client = client;
    }

    FsMatcher withFeatureStructure(Map<String, Object> featureStructure) {
        this.featureStructure = featureStructure;
        return this;
    }

    FsMatcher withAnalysisConfig(AnalysisConfig analysisConfig) {
        this.analysisConfig = analysisConfig;
        return this;
    }

    private void prepare(UnitOfAnalysis target) throws NlpTabException {
        Preconditions.checkNotNull(featureStructure, "featureStructure should be set before calling normal or converse");

        this.target = target;

        @SuppressWarnings("unchecked")
        Map<String, Object> primaryLocation = (Map<String, Object>) featureStructure.get("primaryLocation");

        if (primaryLocation == null || !primaryLocation.containsKey("begin") || !primaryLocation.containsKey("end")) {
            throw new NlpTabException("Primary location is null or is missing begin or end");
        }

        String documentId = (String) featureStructure.get("documentIdentifier");

        if (documentId == null) {
            throw new NlpTabException("feature structure did not have document identifier");
        }

        int begin = (Integer) primaryLocation.get("begin");
        int end = (Integer) primaryLocation.get("end");
        int fuzzDistance = analysisConfig.getFuzzDistance();

        if (fuzzDistance == 0) {
            boolQueryBuilder = target.queryInDocument(documentId)
                    .must(QueryBuilders.termQuery("primaryLocation.begin", begin))
                    .must(QueryBuilders.termQuery("primaryLocation.end", end));
        } else {
            boolQueryBuilder = target.queryInDocument(documentId)
                    .must(QueryBuilders.rangeQuery("primaryLocation.begin")
                            .gte(begin - fuzzDistance)
                            .lte(begin + fuzzDistance))
                    .must(QueryBuilders.rangeQuery("primaryLocation.end")
                            .gte(end - fuzzDistance)
                            .lte(end + fuzzDistance));
        }

    }

    FsMatcher normal() throws NlpTabException {
        Preconditions.checkState(featureStructureTesters == null, "Normal or converse should only be called once");
        Preconditions.checkNotNull(analysisConfig, "analysisConfig should be set before calling normal");

        prepare(analysisConfig.getReference());

        featureStructureTesters = analysisConfig.createFeatureStructureTesters(featureStructure);

        return this;
    }

    FsMatcher converse() throws NlpTabException {
        Preconditions.checkState(featureStructureTesters == null, "Normal or converse should only be called once");
        Preconditions.checkNotNull(analysisConfig, "analysisConfig should be set before calling converse");

        prepare(analysisConfig.getHypothesis());

        featureStructureTesters = analysisConfig.createConverseFeatureStructureTesters(featureStructure);

        return this;
    }

    private void getResults() {
        SearchResponse searchResponse = client.prepareSearch(target.getSystemIndex())
                .setTypes("FeatureStructure")
                .setQuery(boolQueryBuilder)
                .setFrom(from)
                .setSize(SIZE)
                .execute().actionGet();

        SearchHits hits = searchResponse.getHits();

        totalHits = hits.getTotalHits();
        this.hits = hits.getHits();
    }

    @Nullable
    String getMatchingId() throws NlpTabException {
        from = 0;

        getResults();

        while (from < totalHits) {
            for (SearchHit hit : hits) {
                Map<String, Object> featureStructure = hit.getSource();

                Predicate<FeatureValueTester> testFs = featureTester -> featureTester.test(featureStructure);
                if (featureStructureTesters.stream().allMatch(testFs)) {
                    return hit.getId();
                }
            }

            from = from + SIZE;
            getResults();
        }

        return null;
    }
}
