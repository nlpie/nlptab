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

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import java.io.IOException;
import java.util.Map;

/**
 * The path of an analysis.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class UnitOfAnalysis {
    /**
     * The system index to run against.
     */
    private final String systemIndex;

    /**
     * The type to run against.
     */
    private final String type;

    UnitOfAnalysis(String systemIndex, String type) {
        this.systemIndex = systemIndex;
        this.type = type;
    }

    String getSystemIndex() {
        return systemIndex;
    }

    String getType() {
        return type;
    }

    BoolQueryBuilder queryInDocument(String documentId) {
        return QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("types", type))
                .must(QueryBuilders.termQuery("documentIdentifier", documentId));
    }

    void append(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.field("systemIndex", systemIndex)
                .field("typeName", type);
    }

    public static UnitOfAnalysis createFromJsonMap(Map<String, Object> jsonMap) throws AnalysisConfigurationException {
        String selectedSystem = (String) jsonMap.get("selectedSystem");
        String selectedType = (String) jsonMap.get("selectedType");
        if (selectedSystem == null || selectedType == null) {
            throw new AnalysisConfigurationException("system or type were null in unit of analysis config");
        }
        return new UnitOfAnalysis(selectedSystem, selectedType);
    }

    @Override
    public String toString() {
        return "UnitOfAnalysis{" +
                "systemIndex='" + systemIndex + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
