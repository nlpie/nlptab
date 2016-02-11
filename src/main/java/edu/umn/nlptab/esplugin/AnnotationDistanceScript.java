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

package edu.umn.nlptab.esplugin;

import org.elasticsearch.script.AbstractSearchScript;

import java.util.Map;

/**
 *
 */
class AnnotationDistanceScript extends AbstractSearchScript {

    private final int beginParam;

    private final int endParam;

    public AnnotationDistanceScript(Map params) {
        beginParam = (int) params.get("begin");
        endParam = (int) params.get("end");
    }

    @Override
    public Object run() {
        long begin = docFieldLongs("primaryLocation.begin").getValue();
        long end = docFieldLongs("primaryLocation.end").getValue();

        float dist = (float) Math.sqrt(Math.pow(begin - beginParam, 2) + Math.pow(end - endParam, 2));
        return 1.0f / (dist + 1.0f);
    }
}
