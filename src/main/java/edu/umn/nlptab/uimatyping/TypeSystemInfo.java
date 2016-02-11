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

import com.google.common.collect.ImmutableSet;
import edu.umn.nlptab.NlpTabException;
import org.apache.uima.cas.*;

import javax.annotation.Nullable;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

/**
 * Class responsible for storing information for nlp tab about a UIMA type system. Stores information for primitive
 * value adaptation, type filtering, and features for types.
 * <p/>
 * Upon initialization creates the factories responsible for creating value adapters for primitive UIMA values
 * (boolean, integer, float, etc), uima lists of primitives, and uima arrays of primitives.
 */
public class TypeSystemInfo {

    private static final Function<TypeSystem, Function<Type, ValueAdapter>> NULL_ADAPTER = typeSystem -> type -> null;

    private static ValueAdapter createPrimitiveListValueAdapter(TypeSystem typeSystem, Type type) {
        Type floatListType = typeSystem.getType(CAS.TYPE_NAME_FLOAT_LIST);
        Type integerListType = typeSystem.getType(CAS.TYPE_NAME_INTEGER_LIST);
        Type stringListType = typeSystem.getType(CAS.TYPE_NAME_STRING_LIST);

        Feature headFeature = type.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_HEAD);
        Feature tailFeature = type.getFeatureByBaseName(CAS.FEATURE_BASE_NAME_TAIL);
        String nonEmptyName;
        Method headMethod;
        if (typeSystem.subsumes(floatListType, type)) {
            nonEmptyName = CAS.TYPE_NAME_NON_EMPTY_FLOAT_LIST;
            try {
                headMethod = FeatureStructure.class.getMethod("getFloatValue", Feature.class);
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        } else if (typeSystem.subsumes(integerListType, type)) {
            nonEmptyName = CAS.TYPE_NAME_NON_EMPTY_INTEGER_LIST;
            try {
                headMethod = FeatureStructure.class.getMethod("getIntValue", Feature.class);
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        } else if (typeSystem.subsumes(stringListType, type)) {
            nonEmptyName = CAS.TYPE_NAME_NON_EMPTY_STRING_LIST;
            try {
                headMethod = FeatureStructure.class.getMethod("getStringValue", Feature.class);
            } catch (NoSuchMethodException e) {
                throw new AssertionError(e);
            }
        } else {
            throw new AssertionError("PrimitiveListValueAdapter created for improper uima type");
        }
        return new PrimitiveListValueAdapter(headFeature, headMethod, nonEmptyName, tailFeature);
    }

    private static ValueAdapter createPrimitiveValueAdapter(TypeSystem typeSystem, Type type) {
        Method getMethod;
        String key;

        Class<FeatureStructure> clazz = FeatureStructure.class;
        Type parentType = typeSystem.getParent(type);
        String typeName;
        if (parentType != null && !parentType.getName().equals("uima.cas.TOP")) {
            typeName = parentType.getName();
        } else {
            typeName = type.getName();
        }
        try {
            switch (typeName) {
                case CAS.TYPE_NAME_BOOLEAN:
                    getMethod = clazz.getMethod("getBooleanValue", Feature.class);
                    key = "boolean";
                    break;
                case CAS.TYPE_NAME_BYTE:
                    getMethod = clazz.getMethod("getByteValue", Feature.class);
                    key = "byte";
                    break;
                case CAS.TYPE_NAME_DOUBLE:
                    getMethod = clazz.getMethod("getDoubleValue", Feature.class);
                    key = "double";
                    break;
                case CAS.TYPE_NAME_FLOAT:
                    getMethod = clazz.getMethod("getFloatValue", Feature.class);
                    key = "float";
                    break;
                case CAS.TYPE_NAME_INTEGER:
                    getMethod = clazz.getMethod("getIntValue", Feature.class);
                    key = "int";
                    break;
                case CAS.TYPE_NAME_LONG:
                    getMethod = clazz.getMethod("getLongValue", Feature.class);
                    key = "long";
                    break;
                case CAS.TYPE_NAME_SHORT:
                    getMethod = clazz.getMethod("getShortValue", Feature.class);
                    key = "short";
                    break;
                case CAS.TYPE_NAME_STRING:
                    getMethod = clazz.getMethod("getStringValue", Feature.class);
                    key = "string";
                    break;
                default:
                    String msg = "type name not appropriate for primitive type " + typeName;
                    throw new AssertionError(msg);
            }
        } catch (NoSuchMethodException e) {
            throw new AssertionError(e);
        }
        return new PrimitiveValueAdapter(getMethod, key);
    }

    private static final Map<String, Function<TypeSystem, Function<Type, ValueAdapter>>> VALUE_ADAPTERS = valueAdapters();

    private static Map<String, Function<TypeSystem, Function<Type, ValueAdapter>>> valueAdapters() {
        Map<String, Function<TypeSystem, Function<Type, ValueAdapter>>> valueAdapters = new HashMap<>();

        valueAdapters.put(CAS.TYPE_NAME_BOOLEAN_ARRAY, typeSystem -> type ->  PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_BYTE_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_DOUBLE_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_FLOAT_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_INTEGER_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_LONG_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_SHORT_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_STRING_ARRAY, typeSystem -> type -> PrimitiveArrayValueAdapter.INSTANCE);
        valueAdapters.put(CAS.TYPE_NAME_FLOAT_LIST, typeSystem -> type -> createPrimitiveListValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_INTEGER_LIST, typeSystem -> type -> createPrimitiveListValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_STRING_LIST, typeSystem -> type -> createPrimitiveListValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_BOOLEAN, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_BYTE, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_DOUBLE, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_FLOAT, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_INTEGER, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_LONG, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_SHORT, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_STRING, typeSystem -> type -> createPrimitiveValueAdapter(typeSystem, type));
        valueAdapters.put(CAS.TYPE_NAME_TOP, NULL_ADAPTER);
        valueAdapters.put(CAS.TYPE_NAME_ANNOTATION, NULL_ADAPTER);
        valueAdapters.put(CAS.TYPE_NAME_FS_ARRAY, NULL_ADAPTER);
        valueAdapters.put(CAS.TYPE_NAME_FS_LIST, NULL_ADAPTER);

        return Collections.unmodifiableMap(valueAdapters);
    }

    private final TypeSystem typeSystem;
    private final Set<Type> nonPrimitiveTypes;
    private final Lock featuresForTypeCreationLock;
    private final Lock typeValueAdapterCreationLock;
    private final Map<Type, ValueAdapter> valueAdapterMap;
    private final Map<Type, FeaturesForType> featuresForTypeToTypeMap;
    private final ImmutableSet<String> acceptedTypeNames;

    /**
     * Default constructor. Initializes the TypeAdaptersForTypeSystem for a specified type system.
     *
     * @param typeSystem type system to create value adapters for.
     */
    public TypeSystemInfo(TypeSystem typeSystem, ImmutableSet<String> acceptedTypeNames) {
        this.typeSystem = typeSystem;
        nonPrimitiveTypes = Collections.newSetFromMap(new ConcurrentHashMap<>());
        featuresForTypeCreationLock = new ReentrantLock();
        typeValueAdapterCreationLock = new ReentrantLock();
        valueAdapterMap = new ConcurrentHashMap<>();

        featuresForTypeToTypeMap = new ConcurrentHashMap<>();
        this.acceptedTypeNames = acceptedTypeNames;
    }

    /**
     * Returns the ValueAdapter for the type if it is a primitive type.
     *
     * @param type
     * @return
     * @throws NlpTabException
     */
    @Nullable
    public ValueAdapter getValueAdapter(Type type) throws NlpTabException {
        ValueAdapter featureValueAdapter = valueAdapterMap.get(type);
        if (featureValueAdapter == null && !nonPrimitiveTypes.contains(type)) {
            typeValueAdapterCreationLock.lock();
            if (valueAdapterMap.containsKey(type)) { // check a second time after the lock
                featureValueAdapter = valueAdapterMap.get(type);
            } else {
                Function<TypeSystem, Function<Type, ValueAdapter>> adapterFunction = null;
                Type typePointer = type;
                while (adapterFunction == null) {
                    adapterFunction = VALUE_ADAPTERS.get(typePointer.getName());
                    if (adapterFunction == null) {
                        Type parent = typeSystem.getParent(type);
                        if (parent == null || typePointer.getName().equals(parent.getName())) {
                            adapterFunction = NULL_ADAPTER;
                        }
                        typePointer = parent;
                    }
                }
                featureValueAdapter = adapterFunction.apply(typeSystem).apply(type);
                if (featureValueAdapter != null) {
                    valueAdapterMap.put(type, featureValueAdapter);
                } else {
                    nonPrimitiveTypes.add(type);
                }
            }
            typeValueAdapterCreationLock.unlock();
        }
        return featureValueAdapter;
    }

    /**
     * @param type
     * @return
     * @throws NlpTabException
     */
    public FeaturesForType getFeaturesForType(Type type) throws NlpTabException {
        FeaturesForType featuresForType;
        if (!featuresForTypeToTypeMap.containsKey(type)) {
            featuresForTypeCreationLock.lock();
            try {
                if (!featuresForTypeToTypeMap.containsKey(type)) { // check again inside the lock
                    ImmutableSet.Builder<Feature> primitiveFeaturesBuilder = ImmutableSet.builder();
                    ImmutableSet.Builder<Feature> referenceFeaturesBuilder = ImmutableSet.builder();
                    List<Feature> features = type.getFeatures();
                    for (Feature feature : features) {
                        if (getValueAdapter(feature.getRange()) != null) {
                            primitiveFeaturesBuilder.add(feature);
                        } else {
                            referenceFeaturesBuilder.add(feature);
                        }
                    }
                    featuresForType = new FeaturesForType(primitiveFeaturesBuilder.build(), referenceFeaturesBuilder.build());
                    featuresForTypeToTypeMap.put(type, featuresForType);
                }
            } finally {
                featuresForTypeCreationLock.unlock();
            }
        }
        featuresForType = featuresForTypeToTypeMap.get(type);
        return featuresForType;
    }

    /**
     * Checks if the type name is filtered in this type system.
     *
     * @param typeName type name to check
     * @return true if the type is not filtered, false otherwise
     */
    public boolean isTypeAccepted(String typeName) {
        return acceptedTypeNames.contains(typeName);
    }
}
