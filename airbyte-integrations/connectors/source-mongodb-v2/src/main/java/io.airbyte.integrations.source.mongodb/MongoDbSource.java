/*
 * Copyright (c) 2022 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.source.mongodb;

import static com.mongodb.client.model.Filters.gt;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.mongodb.client.MongoCollection;
import io.airbyte.commons.functional.CheckedConsumer;
import io.airbyte.commons.json.Jsons;
import io.airbyte.commons.util.AutoCloseableIterator;
import io.airbyte.commons.util.AutoCloseableIterators;
import io.airbyte.db.jdbc.JdbcUtils;
import io.airbyte.db.mongodb.MongoDatabase;
import io.airbyte.db.mongodb.MongoUtils;
import io.airbyte.db.mongodb.MongoUtils.MongoInstanceType;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.base.Source;
import io.airbyte.integrations.source.relationaldb.AbstractDbSource;
import io.airbyte.integrations.source.relationaldb.TableInfo;
import io.airbyte.protocol.models.CommonField;
import io.airbyte.protocol.models.JsonSchemaType;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MongoDbSource extends AbstractDbSource<BsonType, MongoDatabase> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSource.class);

  private static final String MONGODB_SERVER_URL = "mongodb://%s%s:%s/%s?authSource=%s&ssl=%s";
  private static final String MONGODB_CLUSTER_URL = "mongodb+srv://%s%s/%s?authSource=%s&retryWrites=true&w=majority&tls=true";
  private static final String MONGODB_REPLICA_URL = "mongodb://%s%s/%s?authSource=%s&ssl=%s&directConnection=false";
  private static final String USER = "user";
  private static final String INSTANCE_TYPE = "instance_type";
  private static final String INSTANCE = "instance";
  private static final String CLUSTER_URL = "cluster_url";
  private static final String SERVER_ADDRESSES = "server_addresses";
  private static final String REPLICA_SET = "replica_set";
  private static final String AUTH_SOURCE = "auth_source";
  private static final String PRIMARY_KEY = "_id";
  private static final String ENCRYPTION = "encryption";

  private static final String KEY_STORE_FILE_PATH = "clientkeystore.jks";
  private static final String KEY_STORE_PASS = "changeit";


  public static void main(final String[] args) throws Exception {
    final Source source = new MongoDbSource();
    LOGGER.info("starting source: {}", MongoDbSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", MongoDbSource.class);
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) {
    final List<String> additionalParameters = new ArrayList<>();
    final var credentials = config.has(USER) && config.has(JdbcUtils.PASSWORD_KEY)
        ? String.format("%s:%s@", config.get(USER).asText(), config.get(JdbcUtils.PASSWORD_KEY).asText())
        : StringUtils.EMPTY;

    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("connectionString", buildConnectionString(config, credentials, additionalParameters))
        .put(JdbcUtils.DATABASE_KEY, config.get(JdbcUtils.DATABASE_KEY).asText());

    if (!additionalParameters.isEmpty()) {
      final String connectionParams = String.join(";", additionalParameters);
      configBuilder.put(JdbcUtils.CONNECTION_PROPERTIES_KEY, connectionParams);
    }

    return Jsons.jsonNode(configBuilder.build());
  }

  @Override
  protected MongoDatabase createDatabase(final JsonNode config) throws Exception {
    final var dbConfig = toDatabaseConfig(config);
    return new MongoDatabase(dbConfig.get("connectionString").asText(),
        dbConfig.get(JdbcUtils.DATABASE_KEY).asText());
  }

  @Override
  public List<CheckedConsumer<MongoDatabase, Exception>> getCheckOperations(final JsonNode config)
      throws Exception {
    final List<CheckedConsumer<MongoDatabase, Exception>> checkList = new ArrayList<>();
    checkList.add(database -> {
      if (getAuthorizedCollections(database).isEmpty()) {
        throw new Exception("Unable to execute any operation on the source!");
      } else {
        LOGGER.info("The source passed the basic operation test!");
      }
    });
    return checkList;
  }

  @Override
  protected JsonSchemaType getType(final BsonType fieldType) {
    return MongoUtils.getType(fieldType);
  }

  @Override
  public Set<String> getExcludedInternalNameSpaces() {
    return Collections.emptySet();
  }

  @Override
  protected List<TableInfo<CommonField<BsonType>>> discoverInternal(final MongoDatabase database)
      throws Exception {
    final List<TableInfo<CommonField<BsonType>>> tableInfos = new ArrayList<>();

    for (final String collectionName : getAuthorizedCollections(database)) {
      final MongoCollection<Document> collection = database.getCollection(collectionName);
      final List<CommonField<BsonType>> fields = MongoUtils.getUniqueFields(collection).stream().map(MongoUtils::nodeToCommonField).toList();

      // The field name _id is reserved for use as a primary key;
      final TableInfo<CommonField<BsonType>> tableInfo = TableInfo.<CommonField<BsonType>>builder()
          .nameSpace(database.getName())
          .name(collectionName)
          .fields(fields)
          .primaryKeys(List.of(PRIMARY_KEY))
          .build();

      tableInfos.add(tableInfo);
    }
    return tableInfos;
  }

  private Set<String> getAuthorizedCollections(final MongoDatabase database) {
    /*
     * db.runCommand ({listCollections: 1.0, authorizedCollections: true, nameOnly: true }) the command
     * returns only those collections for which the user has privileges. For example, if a user has find
     * action on specific collections, the command returns only those collections; or, if a user has
     * find or any other action, on the database resource, the command lists all collections in the
     * database.
     */
    final Document document = database.getDatabase().runCommand(new Document("listCollections", 1)
        .append("authorizedCollections", true)
        .append("nameOnly", true))
        .append("filter", "{ 'type': 'collection' }");
    return document.toBsonDocument()
        .get("cursor").asDocument()
        .getArray("firstBatch")
        .stream()
        .map(bsonValue -> bsonValue.asDocument().getString("name").getValue())
        .collect(Collectors.toSet());

  }

  @Override
  protected List<TableInfo<CommonField<BsonType>>> discoverInternal(final MongoDatabase database, final String schema) throws Exception {
    // MondoDb doesn't support schemas
    return discoverInternal(database);
  }

  @Override
  protected Map<String, List<String>> discoverPrimaryKeys(final MongoDatabase database,
                                                          final List<TableInfo<CommonField<BsonType>>> tableInfos) {
    return tableInfos.stream()
        .collect(Collectors.toMap(
            TableInfo::getName,
            TableInfo::getPrimaryKeys));
  }

  @Override
  protected String getQuoteString() {
    return "";
  }

  @Override
  public AutoCloseableIterator<JsonNode> queryTableFullRefresh(final MongoDatabase database,
                                                               final List<String> columnNames,
                                                               final String schemaName,
                                                               final String tableName) {
    return queryTable(database, columnNames, tableName, null);
  }

  @Override
  public AutoCloseableIterator<JsonNode> queryTableIncremental(final MongoDatabase database,
                                                               final List<String> columnNames,
                                                               final String schemaName,
                                                               final String tableName,
                                                               final String cursorField,
                                                               final BsonType cursorFieldType,
                                                               final String cursor) {
    final Bson greaterComparison = gt(cursorField, MongoUtils.getBsonValue(cursorFieldType, cursor));
    return queryTable(database, columnNames, tableName, greaterComparison);
  }

  private AutoCloseableIterator<JsonNode> queryTable(final MongoDatabase database,
                                                     final List<String> columnNames,
                                                     final String tableName,
                                                     final Bson filter) {
    return AutoCloseableIterators.lazyIterator(() -> {
      try {
        final Stream<JsonNode> stream = database.read(tableName, columnNames, Optional.ofNullable(filter));
        return AutoCloseableIterators.fromStream(stream);
      } catch (final Exception e) {
        throw new RuntimeException(e);
      }
    });
  }

  private String buildConnectionString(final JsonNode config, final String credentials, final List<String> additionalParameters) {
    final StringBuilder connectionStrBuilder = new StringBuilder();

    final JsonNode encryptionConfig = config.get("encryption");
    final String encryptionMethod = encryptionConfig.get("encryption_method").asText();
    boolean tls = false;
    switch (encryptionMethod) {
      case "unencrypted" -> {
        tls = false;
        ;
      }
      case "encrypted_verify_certificate" -> {
        try {
          convertAndImportCertificate(encryptionConfig.get("ssl_certificate").asText());
        } catch (final IOException | InterruptedException e) {
          throw new RuntimeException("Failed to import certificate into Java Keystore");
        }
        tls = true;
        additionalParameters.add("javax.net.ssl.trustStore=" + KEY_STORE_FILE_PATH);
        additionalParameters.add("javax.net.ssl.trustStoreType=JKS");
        additionalParameters.add("javax.net.ssl.trustStorePassword=" + KEY_STORE_PASS);
      }
    }


    final JsonNode instanceConfig = config.get(INSTANCE_TYPE);
    final MongoInstanceType instance = MongoInstanceType.fromValue(instanceConfig.get(INSTANCE).asText());
    switch (instance) {
      case STANDALONE -> {
        // supports backward compatibility and secure only connector
        connectionStrBuilder.append(
            String.format(MONGODB_SERVER_URL, credentials, instanceConfig.get(JdbcUtils.HOST_KEY).asText(),
                instanceConfig.get(JdbcUtils.PORT_KEY).asText(),
                config.get(JdbcUtils.DATABASE_KEY).asText(), config.get(AUTH_SOURCE).asText(), tls));
      }
      case REPLICA -> {
        connectionStrBuilder.append(
            String.format(MONGODB_REPLICA_URL, credentials, instanceConfig.get(SERVER_ADDRESSES).asText(),
                config.get(JdbcUtils.DATABASE_KEY).asText(),
                config.get(AUTH_SOURCE).asText(), tls));
        if (instanceConfig.has(REPLICA_SET)) {
          connectionStrBuilder.append(String.format("&replicaSet=%s", instanceConfig.get(REPLICA_SET).asText()));
        }
      }
      case ATLAS -> {
        connectionStrBuilder.append(
            String.format(MONGODB_CLUSTER_URL, credentials, instanceConfig.get(CLUSTER_URL).asText(), config.get(JdbcUtils.DATABASE_KEY).asText(),
                config.get(AUTH_SOURCE).asText()));
      }
      default -> throw new IllegalArgumentException("Unsupported instance type: " + instance);
    }
    return connectionStrBuilder.toString();
  }

  private static void convertAndImportCertificate(final String certificate) throws IOException, InterruptedException {
    final Runtime run = Runtime.getRuntime();
    final String[] cert_list = certificate.split("(?<=/-----END CERTIFICATE-----/)");
    for (int i = 0; i < cert_list.length; i++) {
      try (final PrintWriter out = new PrintWriter("certificate_" + i + ".pem")) {
        out.print(cert_list[i]);
      }
      BufferedReader cert = runProcessWithOutput("openssl x509 -noout -text -in certificate_" + i + ".pem", run);
      Pattern subject_pattern = Pattern.compile("Subject:");
      Pattern pattern = Pattern.compile("(?<=(CN=)).*|(?<=(CN = )).*");
      String alias;
      String line;
      while ((line = cert.readLine()) != null) {
        Matcher subject_matcher = subject_pattern.matcher(line);
        if (subject_matcher.matches()) {
          Matcher matcher = pattern.matcher(line);
          alias = matcher.group();
          runProcess(
            "keytool -import -alias " + alias + " -keystore " + KEY_STORE_FILE_PATH + " -file certificate_" + i + ".pem -storepass " + KEY_STORE_PASS + " -noprompt", 
            run);
          break;
        }
      }
    }
  }

  private static void runProcess(final String cmd, final Runtime run) throws IOException, InterruptedException {
    final Process pr = run.exec(cmd);
    if (!pr.waitFor(30, TimeUnit.SECONDS)) {
      pr.destroy();
      throw new RuntimeException("Timeout while executing: " + cmd);
    } ;
  }

  private static BufferedReader runProcessWithOutput(final String cmd, final Runtime run) throws IOException, InterruptedException {
    final Process pr = run.exec(cmd);
    InputStream is = pr.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(is));
    if (!pr.waitFor(30, TimeUnit.SECONDS)) {
      pr.destroy();
      throw new RuntimeException("Timeout while executing: " + cmd);
    } ;
    return reader;
  }

  @Override
  public void close() throws Exception {}
}
