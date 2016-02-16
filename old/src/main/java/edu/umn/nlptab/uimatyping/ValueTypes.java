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

package edu.umn.nlptab.uimatyping;

import org.apache.uima.cas.CAS;

import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.Map;

import static edu.umn.nlptab.uimatyping.ValueType.*;

/**
 *
 */
public final class ValueTypes {
    private ValueTypes() {
        throw new UnsupportedOperationException();
    }

    private static final Map<String, ValueType> FOR_NAME_MAP = buildForNameMap();

    private static Map<String, ValueType> buildForNameMap() {
        Map<String, ValueType> forNameMap = new HashMap<>();
        forNameMap.put(CAS.TYPE_NAME_BOOLEAN, BOOLEAN);
        forNameMap.put(CAS.TYPE_NAME_BYTE, BYTE);
        forNameMap.put(CAS.TYPE_NAME_SHORT, SHORT);
        forNameMap.put(CAS.TYPE_NAME_INTEGER, INTEGER);
        forNameMap.put(CAS.TYPE_NAME_LONG, LONG);
        forNameMap.put(CAS.TYPE_NAME_FLOAT, FLOAT);
        forNameMap.put(CAS.TYPE_NAME_DOUBLE, DOUBLE);
        forNameMap.put(CAS.TYPE_NAME_STRING, STRING);
        forNameMap.put(CAS.TYPE_NAME_BOOLEAN_ARRAY, BOOLEAN_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_BYTE_ARRAY, BYTE_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_SHORT_ARRAY, SHORT_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_INTEGER_ARRAY, INTEGER_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_LONG_ARRAY, LONG_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_FLOAT_ARRAY, FLOAT_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_DOUBLE_ARRAY, DOUBLE_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_STRING_ARRAY, STRING_ARRAY);
        forNameMap.put(CAS.TYPE_NAME_FLOAT_LIST, FLOAT_LIST);
        forNameMap.put(CAS.TYPE_NAME_INTEGER_LIST, INTEGER_LIST);
        forNameMap.put(CAS.TYPE_NAME_STRING_LIST, STRING_LIST);

        return Collections.unmodifiableMap(forNameMap);
    }

    private static final Map<ValueType, String> KEYS = keys();

    private static Map<ValueType, String> keys() {
        Map<ValueType, String> keys = new EnumMap<>(ValueType.class);

        keys.put(BOOLEAN, "boolean");
        keys.put(BYTE, "byte");
        keys.put(DOUBLE, "double");
        keys.put(FLOAT, "float");
        keys.put(INTEGER, "int");
        keys.put(LONG, "long");
        keys.put(SHORT, "short");
        keys.put(STRING, "string");
        keys.put(BOOLEAN_ARRAY, "BooleanArray");
        keys.put(BYTE_ARRAY, "ByteArray");
        keys.put(SHORT_ARRAY, "ShortArray");
        keys.put(INTEGER_ARRAY, "IntegerArray");
        keys.put(LONG_ARRAY, "LongArray");
        keys.put(FLOAT_ARRAY, "FloatArray");
        keys.put(DOUBLE_ARRAY, "DoubleArray");
        keys.put(STRING_ARRAY, "StringArray");
        keys.put(FLOAT_LIST, "FloatList");
        keys.put(INTEGER_LIST, "IntegerList");
        keys.put(STRING_LIST, "StringList");

        return Collections.unmodifiableMap(keys);
    }

    public static ValueType forName(String name) {
        ValueType valueType = FOR_NAME_MAP.get(name);
        if (valueType == null) {
            throw new IllegalArgumentException("Not a valid name for a primitive type");
        }
        return valueType;
    }

    public static String fieldName(ValueType valueType) {
        String key = KEYS.get(valueType);
        return key + "Features";
    }
}
