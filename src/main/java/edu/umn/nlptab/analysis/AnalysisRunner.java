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

import edu.umn.nlptab.core.ElasticSearchSetup;
import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Main class responsible for running the co-occurrence analysis.
 *
 * @author Ben Knoll
 * @since 1.0
 */
public class AnalysisRunner {
    private static final ESLogger logger = Loggers.getLogger(AnalysisRunner.class);

    private final CoOccurrenceCounts coOccurrenceCounts;

    private final Provider<TypeCoOccurrenceEvaluation> typeCoOccurrenceEvaluationProvider;

    private final Provider<ElasticSearchSetup> esSetupProvider;

    private final Client client;

    private String id;

    private AnalysisConfig analysisConfig;

    @Inject
    AnalysisRunner(CoOccurrenceCounts coOccurrenceCounts,
                   Provider<TypeCoOccurrenceEvaluation> typeCoOccurrenceEvaluationProvider,
                   Provider<ElasticSearchSetup> esSetupProvider,
                   Client client) {
        this.coOccurrenceCounts = coOccurrenceCounts;
        this.typeCoOccurrenceEvaluationProvider = typeCoOccurrenceEvaluationProvider;
        this.esSetupProvider = esSetupProvider;
        this.client = client;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setAnalysisConfig(AnalysisConfig analysisConfig) {
        this.analysisConfig = analysisConfig;
    }

    /**
     * Runs the analysis.
     */
    public void setupIndexAndPerformAnalysis() {
        String analysisIndex = analysisConfig.getInstanceIndexes().analysisIndex();

        try {
            ElasticSearchSetup elasticSearchSetup = esSetupProvider.get();
            elasticSearchSetup.setIndex(analysisIndex);
            elasticSearchSetup.setUpPrimaryIndex("edu/umn/nlptab/analysis/AnalysisSettings.json");
            elasticSearchSetup.setUpPrimaryIndexType("MatchCounts", "edu/umn/nlptab/analysis/MatchCountsMapping.json");
            elasticSearchSetup.setUpPrimaryIndexType("FalsePositive", "edu/umn/nlptab/analysis/FalsePositive.json");
            elasticSearchSetup.setUpPrimaryIndexType("FalseNegative", "edu/umn/nlptab/analysis/FalseNegative.json");
            elasticSearchSetup.setUpPrimaryIndexType("TruePositive", "edu/umn/nlptab/analysis/TruePositive.json");

            performAnalysis();
            client.prepareUpdate(analysisIndex, "AnalysisTask", id)
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("finished", true)
                            .endObject())
                    .get();
        } catch (InterruptedException | IOException | NlpTabException e) {
            logger.error("Failed analysis", e);
            try {
                client.prepareUpdate(analysisIndex, "AnalysisTask", id)
                        .setDoc(XContentFactory.jsonBuilder()
                                .startObject()
                                .field("finished", true)
                                .field("failed", true)
                                .field("error", e.getLocalizedMessage())
                                .endObject())
                        .get();
            } catch (IOException e1) {
                logger.error("Failed to upload failure object", e1);
            }
        }
    }

    private void performAnalysis() throws IOException, InterruptedException, NlpTabException {
        logger.info("Running {} against {}.", analysisConfig.getHypothesis(),
                analysisConfig.getReference());

        String analysisIndex = analysisConfig.getInstanceIndexes().analysisIndex();

        Set<String> hypothesisIdentifiers = getDocumentIdentifiersInSystem(analysisConfig.getHypothesis().getSystemIndex());
        Set<String> referenceIdentifiers = getDocumentIdentifiersInSystem(analysisConfig.getReference().getSystemIndex());
        Set<String> documentIdentifiers = hypothesisIdentifiers.stream()
                .filter(referenceIdentifiers::contains)
                .collect(Collectors.toSet());

        logger.info("Found {} shared documents", documentIdentifiers.size());

        int completed = 0;

        client.prepareIndex(analysisIndex, "AnalysisTask", id)
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("total", documentIdentifiers.size())
                        .field("completed", 0)
                        .field("finished", false)
                        .field("failed", false)
                        .nullField("error")
                        .endObject())
                .get();

        coOccurrenceCounts.setAnalysisConfig(analysisConfig);
        coOccurrenceCounts.setIndex(analysisIndex);
        coOccurrenceCounts.setAnalysisId(id);

        for (String documentIdentifier : documentIdentifiers) {
            logger.debug("Analyzing document: {}", documentIdentifier);

            TypeCoOccurrenceEvaluation typeCoOccurrenceEvaluation = typeCoOccurrenceEvaluationProvider.get();

            if (typeCoOccurrenceEvaluation == null) {
                throw new RuntimeException("Provider for type cooccurrence returned null");
            }

            typeCoOccurrenceEvaluation.setDocumentId(documentIdentifier);
            typeCoOccurrenceEvaluation.setAnalysisId(id);
            typeCoOccurrenceEvaluation.setIndex(analysisIndex);
            typeCoOccurrenceEvaluation.setAnalysisConfig(analysisConfig);

            CoOccurrenceCounts other = typeCoOccurrenceEvaluation.computeCoOccurrenceCounts();
            coOccurrenceCounts.add(other);

            client.prepareUpdate(analysisIndex, "AnalysisTask", id)
                    .setDoc(XContentFactory.jsonBuilder()
                            .startObject()
                            .field("completed", ++completed)
                            .endObject())
                    .get();
        }

        coOccurrenceCounts.buildRequest().get();
    }

    private Set<String> getDocumentIdentifiersInSystem(String systemIndex) {
        Set<String> documentIdentifiers = new HashSet<>();

        SearchResponse scrollResp = client.prepareSearch(analysisConfig.getInstanceIndexes().searchIndex())
                .setTypes("DocumentInSystem")
                .setQuery(QueryBuilders.termQuery("systemIndex", systemIndex))
                .setScroll(new TimeValue(60, TimeUnit.SECONDS))
                .setSize(100)
                .get();

        while (true) {
            for (SearchHit searchHit : scrollResp.getHits().getHits()) {
                documentIdentifiers.add((String) searchHit.getSource().get("documentIdentifier"));
            }

            scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60, TimeUnit.SECONDS)).get();

            if (scrollResp.getHits().getHits().length == 0) {
                break;
            }
        }

        return documentIdentifiers;
    }
}
