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
import java.util.Objects;

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

    @Nullable
    private String documentId;

    private AnalysisConfig analysisConfig;

    private String index;

    private String analysisId;

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
        BulkRequestBuilder bulkRequestBuilder = client.prepareBulk();

        try (FsDataSource firstSource = new FsDataSource(client, Objects.requireNonNull(documentId),
                analysisConfig.getHypothesis())) {
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
                UnitOfAnalysis hypothesis = analysisConfig.getHypothesis();
                matchUploadable.setFirstPath(hypothesis);
                UnitOfAnalysis reference = analysisConfig.getReference();
                matchUploadable.setSecondPath(reference);

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
            try (FsDataSource secondSource = new FsDataSource(client, documentId, analysisConfig.getReference())) {
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

                        UnitOfAnalysis reference = analysisConfig.getReference();
                        UnitOfAnalysis hypothesis = analysisConfig.getHypothesis();
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
