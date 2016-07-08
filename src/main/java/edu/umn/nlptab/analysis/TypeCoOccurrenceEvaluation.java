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

import edu.umn.nlptab.NlpTabException;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.search.SearchHit;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Map;

/**
 * Evaluates one type against another, determining if they exist at the same location.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class TypeCoOccurrenceEvaluation {

    private final Client client;
    private final Provider<FsMatcher> fsMatcherProvider;
    private final CoOccurrenceCounts coOccurrenceCounts;
    private final Provider<MatchUploadable> matchUploadableProvider;
    private final Provider<ClosestFsFinder> closestFsFinderProvider;
    @Nullable private String documentId;
    @Nullable private AnalysisConfig analysisConfig;
    @Nullable private String index;
    @Nullable private String analysisId;

    @Inject
    TypeCoOccurrenceEvaluation(Client client,
                               Provider<FsMatcher> fsMatcherProvider,
                               CoOccurrenceCounts coOccurrenceCounts,
                               Provider<MatchUploadable> matchUploadableProvider,
                               Provider<ClosestFsFinder> closestFsFinderProvider) {
        this.client = client;
        this.fsMatcherProvider = fsMatcherProvider;
        this.coOccurrenceCounts = coOccurrenceCounts;
        this.matchUploadableProvider = matchUploadableProvider;
        this.closestFsFinderProvider = closestFsFinderProvider;
    }

    void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    void setIndex(String index) {
        this.index = index;
    }

    void setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
    }

    void setAnalysisConfig(AnalysisConfig analysisConfig) {
        this.analysisConfig = analysisConfig;
    }

    CoOccurrenceCounts computeCoOccurrenceCounts() throws IOException, InterruptedException, NlpTabException {
        if (analysisConfig == null) {
            throw new IllegalStateException("analysisConfig not initialized");
        }

        if (index == null) {
            throw new IllegalStateException("index not initialized");
        }

        if (analysisId == null) {
            throw new IllegalStateException("analysisId not initialized");
        }

        if (documentId == null) {
            throw new IllegalStateException("documentId not initialized");
        }

        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        UnitOfAnalysis hypothesis = analysisConfig.getHypothesis();
        UnitOfAnalysis reference = analysisConfig.getReference();
        try (FsDataSource firstSource = new FsDataSource(client, documentId, hypothesis)) {
            FsDataSourceIterator firstIterator = new FsDataSourceIterator(firstSource);
            while (firstIterator.hasNext()) {
                SearchHit searchHit = firstIterator.next();
                Map<String, Object> featureStructure = searchHit.getSource();

                FsMatcher fsMatcher = fsMatcherProvider.get()
                        .withAnalysisConfig(analysisConfig)
                        .withFeatureStructure(featureStructure)
                        .normal();

                String matchingId = fsMatcher.getMatchingId();

                MatchUploadable matchUploadable = matchUploadableProvider.get();
                matchUploadable.setIndex(index);
                matchUploadable.setAnalysisId(analysisId);
                matchUploadable.setFirstId(searchHit.getId());
                matchUploadable.setFirstPath(hypothesis);
                matchUploadable.setSecondPath(reference);
                matchUploadable.setDocumentId(documentId);

                @SuppressWarnings("unchecked")
                Map<String, Object> primaryLocation = (Map<String, Object>) featureStructure.get("primaryLocation");

                matchUploadable.setBegin((int) primaryLocation.get("begin"));
                matchUploadable.setEnd((int) primaryLocation.get("end"));
                matchUploadable.setFirstValues(fsMatcher.getHypothesisValues());
                matchUploadable.setSecondValues(fsMatcher.getReferenceValues());
                matchUploadable.setFirstIsPresent(true);
                matchUploadable.setFirstMatches(true);
                matchUploadable.setSecondIsPresent(fsMatcher.hadPresent());
                matchUploadable.setSecondMatches(matchingId != null);

                if (matchingId != null) {
                    coOccurrenceCounts.incrementBoth();

                    matchUploadable.setSecondId(matchingId);
                    matchUploadable.setMatchType(MatchUploadable.MatchType.TRUE_POSITIVE);

                    IndexRequestBuilder indexRequestBuilder = matchUploadable.buildRequest();

                    bulkRequestBuilder.add(indexRequestBuilder);
                } else {
                    String closestId = closestFsFinderProvider.get()
                            .withDocumentId(documentId)
                            .withFeatureStructure(featureStructure)
                            .withTarget(reference)
                            .getClosestId(analysisConfig.isHitMiss() ? 0 : 80);

                    if (!analysisConfig.isHitMiss() || closestId != null) {
                        coOccurrenceCounts.incrementFirstOnly();

                        matchUploadable.setSecondId(closestId);
                        matchUploadable.setMatchType(MatchUploadable.MatchType.FALSE_POSITIVE);

                        IndexRequestBuilder indexRequestBuilder = matchUploadable.buildRequest();

                        bulkRequestBuilder.add(indexRequestBuilder);
                    }
                }

                if (bulkRequestBuilder.numberOfActions() >= 2000) {
                    bulkRequestBuilder.execute();
                    bulkRequestBuilder = client.prepareBulk();
                }
            }
        }

        if (!analysisConfig.isHitMiss()) {
            try (FsDataSource secondSource = new FsDataSource(client, documentId, reference)) {
                FsDataSourceIterator secondIterator = new FsDataSourceIterator(secondSource);

                while (secondIterator.hasNext()) {
                    SearchHit searchHit = secondIterator.next();
                    Map<String, Object> featureStructure = searchHit.getSource();
                    FsMatcher fsMatcher = fsMatcherProvider.get()
                            .withFeatureStructure(featureStructure)
                            .withAnalysisConfig(analysisConfig)
                            .converse();

                    String matchingId = fsMatcher.getMatchingId();
                    if (matchingId == null) {
                        coOccurrenceCounts.incrementSecondOnly();

                        String closestId = closestFsFinderProvider.get()
                                .withDocumentId(documentId)
                                .withFeatureStructure(featureStructure)
                                .withTarget(hypothesis)
                                .getClosestId(80);

                        MatchUploadable matchUploadable = matchUploadableProvider.get();
                        matchUploadable.setIndex(index);
                        matchUploadable.setAnalysisId(analysisId);
                        matchUploadable.setFirstId(closestId);
                        matchUploadable.setFirstPath(hypothesis);
                        matchUploadable.setSecondId(searchHit.getId());
                        matchUploadable.setSecondPath(reference);
                        matchUploadable.setMatchType(MatchUploadable.MatchType.FALSE_NEGATIVE);
                        matchUploadable.setDocumentId(documentId);
                        matchUploadable.setFirstValues(fsMatcher.getReferenceValues());
                        matchUploadable.setSecondValues(fsMatcher.getHypothesisValues());
                        matchUploadable.setFirstIsPresent(fsMatcher.hadPresent());
                        matchUploadable.setFirstMatches(false);
                        matchUploadable.setSecondIsPresent(true);
                        matchUploadable.setSecondMatches(true);

                        @SuppressWarnings("unchecked")
                        Map<String, Object> primaryLocation = (Map<String, Object>) featureStructure.get("primaryLocation");

                        matchUploadable.setBegin(((int) primaryLocation.get("begin")));
                        matchUploadable.setEnd(((int) primaryLocation.get("end")));

                        IndexRequestBuilder indexRequestBuilder = matchUploadable.buildRequest();

                        bulkRequestBuilder.add(indexRequestBuilder);

                        if (bulkRequestBuilder.numberOfActions() >= 2000) {
                            bulkRequestBuilder.execute();
                            bulkRequestBuilder = client.prepareBulk();
                        }
                    }
                }
            }
        }

        if (bulkRequestBuilder.numberOfActions() > 0) {
            bulkRequestBuilder.execute();
        }

        return coOccurrenceCounts;
    }
}
