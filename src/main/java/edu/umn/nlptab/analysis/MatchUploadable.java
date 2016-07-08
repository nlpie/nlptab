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
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;

import javax.annotation.Nullable;
import java.io.IOException;

/**
 *
 */
class MatchUploadable {

    enum MatchType {
        TRUE_POSITIVE,
        FALSE_NEGATIVE,
        FALSE_POSITIVE
    }

    private final Client client;
    @Nullable private String analysisId;
    @Nullable private String index;
    @Nullable private UnitOfAnalysis firstPath;
    @Nullable private String firstId;
    @Nullable private UnitOfAnalysis secondPath;
    @Nullable private String secondId;
    @Nullable private MatchType matchType;
    @Nullable private String documentId;
    private int begin = -1;
    private int end = -1;
    @Nullable private String firstValues;
    @Nullable private String secondValues;
    private boolean firstIsPresent;
    private boolean firstMatches;
    private boolean secondIsPresent;
    private boolean secondMatches;

    @Inject
    MatchUploadable(Client client) {
        this.client = client;
    }

    void setAnalysisId(String analysisId) {
        this.analysisId = analysisId;
    }

    void setIndex(String index) {
        this.index = index;
    }

    void setFirstPath(UnitOfAnalysis firstPath) {
        this.firstPath = firstPath;
    }

    void setSecondPath(UnitOfAnalysis secondPath) {
        this.secondPath = secondPath;
    }

    void setFirstId(@Nullable String firstId) {
        this.firstId = firstId;
    }

    void setSecondId(@Nullable String secondId) {
        this.secondId = secondId;
    }

    void setMatchType(MatchType matchType) {
        this.matchType = matchType;
    }

    void setDocumentId(String documentId) {
        this.documentId = documentId;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    void setFirstValues(String firstValues) {
        this.firstValues = firstValues;
    }

    void setSecondValues(String secondValues) {
        this.secondValues = secondValues;
    }

    private String getElasticSearchType() {
        switch (matchType) {
            case TRUE_POSITIVE:
                return "TruePositive";
            case FALSE_NEGATIVE:
                return "FalseNegative";
            case FALSE_POSITIVE:
                return "FalsePositive";
            default:
                throw new IllegalStateException();
        }
    }

    public void setFirstIsPresent(boolean firstIsPresent) {
        this.firstIsPresent = firstIsPresent;
    }

    public void setFirstMatches(boolean firstMatches) {
        this.firstMatches = firstMatches;
    }

    public void setSecondIsPresent(boolean secondIsPresent) {
        this.secondIsPresent = secondIsPresent;
    }

    public void setSecondMatches(boolean secondMatches) {
        this.secondMatches = secondMatches;
    }

    IndexRequestBuilder buildRequest() throws IOException {
        Preconditions.checkNotNull(firstPath);
        Preconditions.checkNotNull(secondPath);
        return client.prepareIndex(index, getElasticSearchType())
                .setSource(XContentFactory.jsonBuilder()
                        .startObject()
                        .field("analysisId", analysisId)
                        .field("firstSystem", firstPath.getSystemIndex())
                        .field("firstId", firstId)
                        .field("firstIsPresent", firstIsPresent)
                        .field("firstMatches", firstMatches)
                        .field("firstValues", firstValues)
                        .field("secondSystem", secondPath.getSystemIndex())
                        .field("secondId", secondId)
                        .field("secondIsPresent", secondIsPresent)
                        .field("secondMatches", secondMatches)
                        .field("secondValues", secondValues)
                        .field("documentId", documentId)
                        .field("begin", begin)
                        .field("end", end)
                        .endObject());
    }

}
