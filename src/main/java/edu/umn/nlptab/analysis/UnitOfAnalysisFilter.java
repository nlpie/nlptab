package edu.umn.nlptab.analysis;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import javax.annotation.Nullable;
import java.util.Map;

/**
 *
 */
class UnitOfAnalysisFilter {
    private final Feature feature;

    @Nullable
    private String filterOption;

    @Nullable
    private String value;

    @Inject
    UnitOfAnalysisFilter(Feature feature) {
        this.feature = feature;
    }

    void initFromJsonMap(Map<String, Object> json) {
        filterOption = (String) json.get("option");
        value = (String) json.get("value");
        feature.initFromJsonMap(json);
    }

    QueryBuilder buildQuery() {
        if ("in".equals(filterOption)) {
            return QueryBuilders.queryStringQuery(feature.fullLucenePath() + ":(" + value + ")");
        } else if ("equals".equals(filterOption)) {
            return QueryBuilders.queryStringQuery(feature.fullLucenePath() + ":" + value);
        } else {
            throw new IllegalStateException("Unknown equivalence option");
        }
    }
}
