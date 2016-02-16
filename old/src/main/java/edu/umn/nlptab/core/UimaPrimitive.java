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

package edu.umn.nlptab.core;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Optional;

public class UimaPrimitive implements ToXContent {

    private final String key;


    private final Optional<?> optionalValue;

    public UimaPrimitive(@Nullable Object uimaPrimitiveValue, String key) {
        optionalValue = Optional.ofNullable(uimaPrimitiveValue);
        this.key = key;
    }

    /**
     * The type short name of the primitive e.g. Boolean, Integer, Float etc
     * @return String type short name of the primitive
     */
    public String getKey() {
        return key;
    }

    @Nullable
    public Object getValueOrNull() {
        return optionalValue.orElse(null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (optionalValue.isPresent()) {
            Object value = optionalValue.get();
            builder.value(value);
        }
        return builder;
    }
}
