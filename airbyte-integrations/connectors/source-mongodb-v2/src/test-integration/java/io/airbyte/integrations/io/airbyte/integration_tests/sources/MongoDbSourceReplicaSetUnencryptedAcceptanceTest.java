/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.io.airbyte.integration_tests.sources;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.commons.json.Jsons;

public class MongoDbSourceReplicaSetAcceptanceTestUnencrypted extends MongoDbSourceReplicaSetAcceptanceTest {
    
    credentialsPath = Path.of("secrets/rs_credentials_unencrypted.json");

    @Override
    protected JsonNode getEncryptionConfig(JsonNode credentialsJson) {
        final JsonNode encryptionConfig = Jsons.jsonNode(ImmutableMap.builder()
            .put("encryption_method", "unencrypted")
            .build());
        return encryptionConfig;
    }
}