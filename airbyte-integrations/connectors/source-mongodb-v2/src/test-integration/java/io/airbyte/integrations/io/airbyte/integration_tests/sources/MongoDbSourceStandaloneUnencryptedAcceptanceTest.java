public class MongoDbSourceStandaloneUnencryptedAcceptanceTest extends MongoDbSourceStandaloneAcceptanceTest {
    credentialsPath = Path.of("secrets/rs_credentials_encrypted_custom.json");

    @Override
    protected JsonNode getEncryptionConfig(JsonNode credentialsJson) {
        final JsonNode encryptionConfig = Jsons.jsonNode(ImmutableMap.builder()
            .put("encryption_method", "unencrypted")
            .build());
        return encryptionConfig;
    }
}