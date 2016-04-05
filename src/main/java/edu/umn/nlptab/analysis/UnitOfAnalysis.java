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

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The path of an analysis.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class UnitOfAnalysis {
    private final Provider<UnitOfAnalysisFilter> unitOfAnalysisFilterProvider;

    /**
     * The system index to run against.
     */
    @Nullable
    private String systemIndex;

    /**
     * The type to run against.
     */
    @Nullable
    private String type;

    /**
     * Filters to apply
     */
    @Nullable
    private List<UnitOfAnalysisFilter> analysisFilters;

    @Inject
    UnitOfAnalysis(Provider<UnitOfAnalysisFilter> unitOfAnalysisFilterProvider) {
        this.unitOfAnalysisFilterProvider = unitOfAnalysisFilterProvider;
    }

    String getSystemIndex() {
        if (systemIndex == null) {
            throw new IllegalStateException("system index not initialized");
        }
        return systemIndex;
    }

    String getType() {
        if (type == null) {
            throw new IllegalStateException("type not initialized");
        }
        return type;
    }

    BoolQueryBuilder queryInDocument(String documentId) {
        if (type == null) {
            throw new IllegalStateException("type not initialized");
        }
        if (analysisFilters == null) {
            throw new IllegalStateException("analysisFilters not initialized");
        }

        BoolQueryBuilder boolQuery = QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("types", type))
                .must(QueryBuilders.termQuery("documentIdentifier", documentId));
        for (UnitOfAnalysisFilter analysisFilter : analysisFilters) {
            boolQuery.must(analysisFilter.buildQuery());
        }
        return boolQuery;
    }

    void appendTo(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.field("systemIndex", systemIndex)
                .field("typeName", type);
    }

    void initFromJsonMap(Map<String, Object> jsonMap) throws AnalysisConfigurationException {
        systemIndex = (String) jsonMap.get("selectedSystem");
        type = (String) jsonMap.get("selectedType");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> filtersJson = (List<Map<String, Object>>) jsonMap.get("filters");
        analysisFilters = new ArrayList<>(filtersJson.size());
        for (Map<String, Object> filterObject : filtersJson) {
            UnitOfAnalysisFilter unitOfAnalysisFilter = unitOfAnalysisFilterProvider.get();
            unitOfAnalysisFilter.initFromJsonMap(filterObject);
            analysisFilters.add(unitOfAnalysisFilter);
        }
    }

    @Override
    public String toString() {
        return "UnitOfAnalysis{" +
                "systemIndex='" + systemIndex + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
