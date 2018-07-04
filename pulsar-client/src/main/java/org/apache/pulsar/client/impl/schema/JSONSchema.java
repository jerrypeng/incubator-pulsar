/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.client.impl.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.module.jsonSchema.JsonSchema;
import com.fasterxml.jackson.module.jsonSchema.JsonSchemaGenerator;
import org.apache.avro.reflect.ReflectData;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.SchemaSerializationException;
import org.apache.pulsar.common.schema.SchemaInfo;
import org.apache.pulsar.common.schema.SchemaType;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class JSONSchema<T> implements Schema<T>{

    private final org.apache.avro.Schema schema;
    private final SchemaInfo schemaInfo;
    private final ObjectMapper objectMapper;
    private final Class<T> pojo;
    private Map<String, String> properties;

    private JSONSchema(Class<T> pojo, Map<String, String> properties) {
        this.pojo = pojo;
        this.properties = properties;
        this.objectMapper = new ObjectMapper();

        this.schema = ReflectData.AllowNull.get().getSchema(pojo);
        this.schemaInfo = new SchemaInfo();
        this.schemaInfo.setName("");
        this.schemaInfo.setProperties(properties);
        this.schemaInfo.setType(SchemaType.JSON);
        this.schemaInfo.setSchema(this.schema.toString().getBytes());
    }

    @Override
    public byte[] encode(T message) throws SchemaSerializationException {
        try {
            return objectMapper.writeValueAsBytes(message);
        } catch (JsonProcessingException e) {
            throw new SchemaSerializationException(e);
        }
    }

    @Override
    public T decode(byte[] bytes) {
        try {
            return objectMapper.readValue(new String(bytes), pojo);
        } catch (IOException e) {
            throw new RuntimeException(new SchemaSerializationException(e));
        }
    }

    @Override
    public SchemaInfo getSchemaInfo() {
        return this.schemaInfo;
    }

    /**
     * Implemented for backwards compatibility reasons
     * since the original schema generated by JSONSchema was based off the json schema standard
     * since then we have standardized on Avro
     * @return
     */
    public SchemaInfo getBackwardsCompatibleJsonSchemaInfo() {
        SchemaInfo backwardsCompatibleSchemaInfo;
        try {
            JsonSchemaGenerator schemaGen = new JsonSchemaGenerator(objectMapper);
            JsonSchema jsonBackwardsCompatibileSchema = schemaGen.generateSchema(pojo);
            backwardsCompatibleSchemaInfo = new SchemaInfo();
            backwardsCompatibleSchemaInfo.setName("");
            backwardsCompatibleSchemaInfo.setProperties(properties);
            backwardsCompatibleSchemaInfo.setType(SchemaType.JSON);
            backwardsCompatibleSchemaInfo.setSchema(objectMapper.writeValueAsBytes(jsonBackwardsCompatibileSchema));
        } catch (JsonProcessingException ex) {
            throw new RuntimeException(ex);
        }
        return backwardsCompatibleSchemaInfo;
    }

    public static <T> JSONSchema<T> of(Class<T> pojo) {
        return new JSONSchema<>(pojo, Collections.emptyMap());
    }

    public static <T> JSONSchema<T> of(Class<T> pojo, Map<String, String> properties) {
        return new JSONSchema<>(pojo, properties);
    }
}
