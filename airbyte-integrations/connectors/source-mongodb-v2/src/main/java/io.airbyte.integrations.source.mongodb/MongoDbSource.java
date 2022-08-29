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
import java.io.File;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
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
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;


public class MongoDbSource extends AbstractDbSource<BsonType, MongoDatabase> {

  private static final Logger LOGGER = LoggerFactory.getLogger(MongoDbSource.class);

  private static final String MONGODB_SERVER_URL = "mongodb://%s%s:%s/%s?authSource=%s&ssl=%s";
  private static final String MONGODB_CLUSTER_URL = "mongodb+srv://%s%s/%s?authSource=%s&retryWrites=true&w=majority&ssl=%s";
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
  private static final String KEY_STORE_PASS = "changeit";


  public static void main(final String[] args) throws Exception {
    final Source source = new MongoDbSource();
    LOGGER.info("spec: {}", source.spec().getConnectionSpecification());
    LOGGER.info("starting source: {}", MongoDbSource.class);
    new IntegrationRunner(source).run(args);
    LOGGER.info("completed source: {}", MongoDbSource.class);
  }

  @Override
  public JsonNode toDatabaseConfig(final JsonNode config) throws IllegalStateException {
    LOGGER.info("starting toDatabaseConfig");
    Iterator<String> fields = config.fieldNames();
    while (fields.hasNext()) {
      LOGGER.info("Next fieldName: {}", fields.next());
    }
    boolean isAtlas = config.get(INSTANCE_TYPE).get(INSTANCE).asText().equals("atlas");
    boolean isUnencrypted = config.get(ENCRYPTION).get("encryption_method").asText().equals("unencrypted");
    if (isAtlas && isUnencrypted) {
      throw new IllegalStateException("Atlas instances must use encryption");
    }
    LOGGER.info("passwordkey: {}", JdbcUtils.PASSWORD_KEY.equals("password"));
    final List<String> additionalParameters = new ArrayList<>();
    final var credentials = config.has(USER) && config.has(JdbcUtils.PASSWORD_KEY)
        ? String.format("%s:%s@", config.get(USER).asText(), config.get(JdbcUtils.PASSWORD_KEY).asText())
        : StringUtils.EMPTY;

    final ImmutableMap.Builder<Object, Object> configBuilder = ImmutableMap.builder()
        .put("connectionString", buildConnectionString(config, credentials, additionalParameters))
        .put(JdbcUtils.DATABASE_KEY, config.get(JdbcUtils.DATABASE_KEY).asText());

    LOGGER.info("additionalParameters length: {}", additionalParameters.size());
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
    LOGGER.info("starting buildConnectionString");
    final StringBuilder connectionStrBuilder = new StringBuilder();
    final JsonNode instanceConfig = config.get(INSTANCE_TYPE);
    boolean tls = false;
    // supports backward compatibility and secure only connector
    if (config.has(ENCRYPTION)) {
      final JsonNode encryptionConfig = config.get(ENCRYPTION);
      final String encryptionMethod = encryptionConfig.get("encryption_method").asText();
      switch (encryptionMethod) {
        case "unencrypted" -> {
          tls = false;
        }
        case "encrypted_verify_certificate" -> {
          try {
            convertAndImportCertificate(encryptionConfig.get("ssl_certificate").asText());
          } catch (final IOException | InterruptedException e) {
            throw new RuntimeException("Failed to import certificate into Java Keystore");
          }
          tls = true;
        }
      }
    } else {
        tls = config.has(JdbcUtils.TLS_KEY) ? config.get(JdbcUtils.TLS_KEY).asBoolean()
          : (instanceConfig.has(JdbcUtils.TLS_KEY) ? instanceConfig.get(JdbcUtils.TLS_KEY).asBoolean() : true);
    }

    final MongoInstanceType instance = MongoInstanceType.fromValue(instanceConfig.get(INSTANCE).asText());
    switch (instance) {
      case STANDALONE -> {
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
                config.get(AUTH_SOURCE).asText(), tls));
      }
      default -> throw new IllegalArgumentException("Unsupported instance type: " + instance);
    }
    LOGGER.info("ending buildConnectionString");
    return connectionStrBuilder.toString();
  }

  private static void convertAndImportCertificate(final String certificate) throws IOException, InterruptedException {
    final Runtime run = Runtime.getRuntime();
    final String[] certList = certificate.split("(?<=-----END CERTIFICATE-----)");
    LOGGER.info("cert_list len: {}", certList.length);
    for (int i = 0; i < certList.length; i++) {
      try (final PrintWriter out = new PrintWriter("certificate_" + i + ".pem", StandardCharsets.UTF_8)) {
        out.print(certList[i]);
      }
      BufferedReader cert = runProcessWithOutput("openssl x509 -noout -text -in certificate_" + i + ".pem", run);
      LOGGER.info("Ran openssl");
      Pattern subjectPattern = Pattern.compile("[ \t]*Subject:.*");
      Pattern pattern = Pattern.compile("(?<=(CN=)).*|(?<=(CN = )).*");
      String alias;
      String line;
      while ((line = cert.readLine()) != null) {
        LOGGER.info("Checking line");
        Matcher subjectMatcher = subjectPattern.matcher(line);
        if (subjectMatcher.matches()) {
          Matcher matcher = pattern.matcher(line);
          matcher.find();
          alias = matcher.group();
          LOGGER.info("alias: {}", alias);
          String[] processString = {"keytool",
                                    "-import",
                                    "-alias",
                                    alias,
                                    "-cacerts",
                                    "-file",
                                    "certificate_" + i + ".pem",
                                    "-storepass",
                                    KEY_STORE_PASS,
                                    "-noprompt"};
          runProcess(processString, run);
          break;
        }
      }
      cert.close();
      File f = new File("certificate_" + i + ".pem");
      f.delete();
    }
    BufferedReader cert_output = runProcessWithOutput("keytool -list -cacerts -storepass " + KEY_STORE_PASS, run);
    String cert_line;
    while ((cert_line = cert_output.readLine()) != null) {
      LOGGER.info(cert_line);
    }
    cert_output.close();
  }

  private static void runProcess(final String[] processString, final Runtime run) throws IOException, InterruptedException {
    LOGGER.info("processString: {}", processString[1]);
    final Process pr = run.exec(processString);
    if (!pr.waitFor(30, TimeUnit.SECONDS)) {
      pr.destroy();
      throw new RuntimeException("Timeout while executing: " + processString);
    } ;
    LOGGER.info("exit code: {}", pr.exitValue());
    InputStream error = pr.getErrorStream();
    InputStream input_stream = pr.getInputStream();
    LOGGER.info("error stream: {}", IOUtils.toString(error, StandardCharsets.UTF_8));
    LOGGER.info("input_stream stream: {}", IOUtils.toString(input_stream, StandardCharsets.UTF_8));
  }

  private static BufferedReader runProcessWithOutput(final String cmd, final Runtime run) throws IOException, InterruptedException {
    LOGGER.info("cmd: {}", cmd);
    final Process pr = run.exec(cmd);
    InputStream is = pr.getInputStream();
    BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
    if (!pr.waitFor(30, TimeUnit.SECONDS)) {
      pr.destroy();
      throw new RuntimeException("Timeout while executing: " + cmd);
    } ;
    LOGGER.info("exit code: {}", pr.exitValue());
    return reader;
  }

  @Override
  public void close() throws Exception {}
}
