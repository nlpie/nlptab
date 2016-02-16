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

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 *
 */
class NestedPathSearch {
    private final Client client;

    private String system;

    @Nullable
    private List<String> path;

    @Inject
    NestedPathSearch(Client client) {
        this.client = client;
    }

    NestedPathSearch withSystem(String system) {
        this.system = system;
        return this;
    }

    NestedPathSearch withPath(@Nullable List<String> path) {
        this.path = path;
        return this;
    }

    private Map<String, Object> getSource(String id) {
        if (id == null) {
            return ImmutableMap.of();
        } else {
            GetResponse getResponse = client.prepareGet(system, "FeatureStructure", id)
                    .execute().actionGet();
            return getResponse.getSource();
        }
    }


    private Stream<Map<String, Object>> sources(Map<String, Object> source, int level) {
        if (source.containsKey("arrayItems")) {
            @SuppressWarnings("unchecked")
            List<String> ids = (List<String>) source.get("arrayItems");
            return ids.stream().map(this::getSource).flatMap(s -> sources(s, level));
        } else if (source.containsKey("listItems")) {
            @SuppressWarnings("unchecked")
            List<String> ids = (List<String>) source.get("listItems");
            return ids.stream().map(this::getSource).flatMap(s -> sources(s, level));
        } else {
            if (level == path.size()) {
                return Stream.of(source);
            } else {
                @SuppressWarnings("unchecked")
                Map<String, Object> references = (Map<String, Object>) source.get("references");
                String refFeature = path.get(level);
                String id = (String) references.get(refFeature.replace('.', '_').replace(':', ';'));
                Map<String, Object> childSource = getSource(id);
                if (childSource == null) {
                    return Stream.empty();
                } else {
                    return sources(childSource, level + 1);
                }
            }
        }
    }

    Stream<Map<String, Object>> sources(Map<String, Object> original) {
        return sources(original, 0);
    }
}
