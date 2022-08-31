public class MongoDbSourceStandaloneEncryptedAcceptanceTest extends MongoDbSourceStandaloneAcceptanceTest {
    credentialsPath = Path.of("secrets/rs_credentials_encrypted_custom.json");

    @Override
    protected JsonNode getEncryptionConfig(JsonNode credentialsJson) {
        final JsonNode encryptionConfig = Jsons.jsonNode(ImmutableMap.builder()
            .put("encryption_method", "encrypted_verify_certificate")
            .build());
        return encryptionConfig;
    }
}