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

import edu.umn.nlptab.uimatyping.ValueType;
import edu.umn.nlptab.uimatyping.ValueTypes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Provider;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 *
 */
class Feature {
    private final Provider<NestedPathSearch> nestedPathSearchProvider;

    private String featureName;

    private String luceneFeatureName;

    private String fieldName;

    private String systemIndex;

    @Nullable
    private List<String> nestedStructure;

    @Inject
    Feature(Provider<NestedPathSearch> nestedPathSearchProvider) {
        this.nestedPathSearchProvider = nestedPathSearchProvider;
    }

    void initFromJsonMap(Map<String, Object> map) {
        Map<String, Object> feature = (Map<String, Object>) map.get("feature");

        String name = (String) feature.get("name");
        featureName = name;
        luceneFeatureName = name.replace(".", "_").replace(':', ';');

        String valueTypeString = (String) feature.get("valueType");
        ValueType valueType = ValueTypes.forName(valueTypeString);
        fieldName = ValueTypes.fieldName(valueType);

        nestedStructure = (List<String>) map.get("nestedStructure");
    }

    Feature withSystemIndex(String systemIndex) {
        this.systemIndex = systemIndex;
        return this;
    }

    @Nullable
    private Object value(Map<String, Object> featureStructure) {
        Map<String, Object> featureField = (Map<String, Object>) featureStructure.get(fieldName);
        if (featureField == null) {
            return null;
        } else {
            return featureField.get(luceneFeatureName);
        }
    }

    Object getValueFromFeatureStructure(Map<String, Object> featureStructure) {
        if (nestedStructure != null && nestedStructure.size() > 0) {
            List collection = nestedPathSearchProvider.get()
                    .withSystem(systemIndex)
                    .withPath(nestedStructure)
                    .sources(featureStructure)
                    .map(this::value)
                    .flatMap(value -> value instanceof Collection ? ((Collection<?>)value).stream() : Stream.of(value))
                    .collect(Collectors.toList());
            if (collection.size() == 1) {
                return collection.get(0);
            } else {
                return collection;
            }
        } else {
            return value(featureStructure);
        }
    }

    public void addToXContent(XContentBuilder xContentBuilder) throws IOException {
        xContentBuilder.field("featureName", featureName);
        if (nestedStructure != null) {
            xContentBuilder.array("nestedStructure", nestedStructure.toArray());
        }
    }
}
