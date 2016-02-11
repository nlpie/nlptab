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
import org.elasticsearch.common.inject.Inject;

/**
 * Performs setup for the running of an analysis task, including creating the analysis index.
 *
 * @author Ben Knoll
 * @since 1.0
 */
class AnalysisSetup {
    private final ElasticSearchSetup elasticSearchSetup;

    @Inject
    AnalysisSetup(ElasticSearchSetup elasticSearchSetup) {
        this.elasticSearchSetup = elasticSearchSetup;
    }

    void setUpElasticSearch() throws NlpTabException {
        elasticSearchSetup.setUpPrimaryIndex("edu/umn/nlptab/analysis/AnalysisSettings.json");

        elasticSearchSetup.setUpPrimaryIndexType("MatchCounts",
                "edu/umn/nlptab/analysis/MatchCountsMapping.json");
    }
}
