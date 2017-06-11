package org.embulk.output.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchWriteItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.amazonaws.services.dynamodbv2.model.AttributeDefinition;
import com.amazonaws.services.dynamodbv2.model.CreateTableRequest;
import com.amazonaws.services.dynamodbv2.model.KeySchemaElement;
import com.amazonaws.services.dynamodbv2.model.KeyType;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;
import com.amazonaws.services.dynamodbv2.model.ScalarAttributeType;
import com.amazonaws.services.dynamodbv2.model.TableDescription;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.google.common.base.Optional;
import com.google.inject.Inject;
import org.embulk.config.ConfigException;
import org.embulk.config.UserDataException;
import org.embulk.spi.Exec;
import org.jruby.embed.ScriptingContainer;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class DynamodbUtils
{
    private final Logger log;

    @Inject
    public DynamodbUtils()
    {
        log = Exec.getLogger(getClass());
    }

    protected DynamoDB createDynamoDB(DynamodbOutputPlugin.PluginTask task)
    {
        DynamoDB dynamoDB;
        try {
            AmazonDynamoDBClient client = new AmazonDynamoDBClient(
                    getCredentialsProvider(task),
                    getClientConfiguration(task)
            ).withRegion(Regions.fromName(task.getRegion()));

            if (task.getEndpoint().isPresent()) {
                client.setEndpoint(task.getEndpoint().get());
            }

            dynamoDB = new DynamoDB(client);
            dynamoDB.getTable(task.getTable());
        }
        catch (AmazonServiceException ex) {
            int statusCode = ex.getStatusCode();
            if (statusCode == 400) {
                throw new ConfigException(ex);
            }
            else {
                throw new ConnectionException(ex);
            }
        }
        catch (AmazonClientException ex) {
            throw new ConnectionException(ex);
        }
        return dynamoDB;
    }

    protected ClientConfiguration getClientConfiguration(DynamodbOutputPlugin.PluginTask task)
    {
        ClientConfiguration clientConfig = new ClientConfiguration();

        //clientConfig.setProtocol(Protocol.HTTP);
        clientConfig.setMaxConnections(50); // SDK default: 50
        clientConfig.setMaxErrorRetry(3); // SDK default: 3
        clientConfig.setSocketTimeout(8 * 60 * 1000); // SDK default: 50*1000

        return clientConfig;
    }

    private AWSCredentialsProvider getCredentialsProvider(DynamodbOutputPlugin.PluginTask task)
    {
        return AwsCredentials.getAWSCredentialsProvider(task);
    }

    protected void configCheck(DynamodbOutputPlugin.PluginTask task)
    {
        // @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
        if (task.getMode().equals(DynamodbOutputPlugin.Mode.UPSERT)) {
            if (task.getMaxPutItems() > 25) {
                throw new ConfigException("'max_put_items' must less than or equal to 25");
            }
        }

        if (task.getMode().equals(DynamodbOutputPlugin.Mode.UPSERT_WITH_EXPRESSION)) {
            if (!task.getUpdateExpression().isPresent()) {
                throw new ConfigException("'update_expression' is required when update mode");
            }
        }

        if (task.getAutoCreateTable()) {
            if (!task.getPrimaryKey().isPresent() || !task.getPrimaryKeyType().isPresent()) {
                throw new ConfigException("If auto_create_table is true, both primary_key and primary_key_type is necessary");
            }
            if (!task.getReadCapacityUnits().isPresent() || !task.getWriteCapacityUnits().isPresent()) {
                throw new ConfigException("If auto_create_table is true, 'read_capacity_units' and 'write_capacity_units' is required.");
            }
        }
    }

    protected void batchWriteItem(DynamoDB dynamoDB, TableWriteItems items)
    {
        BatchWriteItemOutcome outcome = dynamoDB.batchWriteItem(items);
        int retryCount = 0;
        try {
            do {
                Map<String, List<WriteRequest>> unprocessedItems = outcome.getUnprocessedItems();
                // @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
                // If DynamoDB returns any unprocessed items, you should retry the batch operation on those items.
                // However, we strongly recommend that you use an exponential backoff algorithm
                if (outcome.getUnprocessedItems().size() > 0) {
                    retryCount++;
                    if (retryCount >= 5) {
                        throw new ConnectionException("Retry count expired while executing batchWriteItem");
                    }
                    Thread.sleep(500 * retryCount);
                    log.warn("Retrieving the unprocessed items");
                    outcome = dynamoDB.batchWriteItemUnprocessed(unprocessedItems);
                }
            } while (outcome.getUnprocessedItems().size() > 0);
        }
        catch (InterruptedException ex) {
            throw new ConnectionException("Retry batchWriteItem was interrupted");
        }
    }

    protected void updateItem(DynamoDB dynamoDB, String tableName, Item item, List primaryKeyelements, Optional<String> expression)
    {
        String hashKeyname = null;
        String rangeKeyname = null;
        Object hashKeyValue = null;
        Object rangeKeyValue = null;
        Map<String, String> attributeNames = new HashMap<>();
        Map<String, Object> attributeValues = new HashMap<>();

        Iterator it = primaryKeyelements.iterator();
        while (it.hasNext()) {
            KeySchemaElement element = (KeySchemaElement) it.next();
            if (element.getKeyType().equals(KeyType.HASH.toString())) {
                hashKeyname = element.getAttributeName();
            }
            else if (element.getKeyType().equals(KeyType.RANGE.toString())) {
                rangeKeyname = element.getAttributeName();
            }
        }

        Map<String, Object> itemMap = item.asMap();
        for (Map.Entry<String, Object> e : itemMap.entrySet()) {
            String keyName = e.getKey();
            if (keyName.equals(hashKeyname)) {
                hashKeyValue = e.getValue();
            }
            else if (keyName.equals(rangeKeyname)) {
                rangeKeyValue = e.getValue();
            }
            else {
                if (expression.get().indexOf(keyName) > 0) {
                    attributeNames.put("#" + keyName, keyName);
                    attributeValues.put(":" + keyName, e.getValue());
                }
            }
        }
        log.debug("attribute names: " + attributeNames.toString());
        log.debug("attribute values: " + attributeValues.toString());

        Table table = dynamoDB.getTable(tableName);

        log.debug(String.format("hash key %s:%s", hashKeyname, hashKeyValue));
        if (rangeKeyValue == null) {
            table.updateItem(hashKeyname, hashKeyValue, expression.get(), attributeNames, attributeValues);
        }
        else {
            log.debug(String.format("range key %s:%s", rangeKeyname, rangeKeyValue));
            table.updateItem(hashKeyname, hashKeyValue, rangeKeyname, rangeKeyValue, expression.get(), attributeNames, attributeValues);
        }
    }

    protected String getPrimaryKeyName(DynamoDB dynamoDB, String tableName)
    {
        Table table = dynamoDB.getTable(tableName);

        TableDescription description = table.describe();
        Iterator<KeySchemaElement> schema = description.getKeySchema().iterator();
        String primaryKey = null;
        while (schema.hasNext()) {
            KeySchemaElement element = schema.next();
            if (element.getKeyType().equals(KeyType.HASH.toString())) {
                primaryKey = element.getAttributeName();
            }
        }
        return primaryKey;
    }

    protected List getPrimaryKey(DynamoDB dynamoDB, String tableName)
    {
        log.debug("getPrimaryKey() called");
        Table table = dynamoDB.getTable(tableName);
        TableDescription description = table.describe();
        List<KeySchemaElement>  keyelements = description.getKeySchema();
        return keyelements;
    }

    protected void createTable(DynamoDB dynamoDB, DynamodbOutputPlugin.PluginTask task)
            throws InterruptedException
    {
        String tableName = task.getTable();
        if (isExistsTable(dynamoDB, tableName)) {
            log.info("Table[{}] is already exists", tableName);
            return;
        }
        ArrayList<KeySchemaElement> keySchema = getKeySchemaElements(task);
        ArrayList<AttributeDefinition> attributeDefinitions = getAttributeDefinitions(task);
        ProvisionedThroughput provisionedThroughput = new ProvisionedThroughput()
                .withReadCapacityUnits(task.getReadCapacityUnits().get().getNormal().get())
                .withWriteCapacityUnits(task.getWriteCapacityUnits().get().getNormal().get());

        dynamoDB.createTable(new CreateTableRequest()
                .withTableName(tableName)
                .withKeySchema(keySchema)
                .withAttributeDefinitions(attributeDefinitions)
                .withProvisionedThroughput(provisionedThroughput)
        );

        Table table = dynamoDB.getTable(tableName);
        table.waitForActive();
        log.info("Created table[{}]", tableName);
    }

    protected void deleteTable(DynamoDB dynamoDB, String tableName)
            throws InterruptedException
    {
        Table table = dynamoDB.getTable(tableName);
        table.delete();
        table.waitForDelete();
        log.info(String.format("Deleted table '%s'", tableName));
    }

    protected boolean isExistsTable(DynamoDB dynamoDB, String tableName)
            throws InterruptedException
    {
        Table table = dynamoDB.getTable(tableName);
        try {
            switch (table.describe().getTableStatus()) {
                case "CREATING":
                case "UPDATING":
                    table.waitForActive();
                    return true;
                case "DELETING":
                    table.waitForDelete();
                    return true;
                default:
                    return true;
            }
        }
        catch (ResourceNotFoundException e) {
            return false;
        }
        catch (AmazonClientException e) {
            return false;
        }
    }

    protected void updateTableProvision(DynamoDB dynamoDB, DynamodbOutputPlugin.PluginTask task, boolean isRaise)
            throws InterruptedException
    {
        if (!task.getReadCapacityUnits().isPresent() && !task.getWriteCapacityUnits().isPresent()) {
            return;
        }

        Boolean isNeedChange = false;

        Table table = dynamoDB.getTable(task.getTable());
        TableDescription description = table.describe();
        long currentReadCapacityUnit = description.getProvisionedThroughput().getReadCapacityUnits();
        long currentWriteCapacityUnit = description.getProvisionedThroughput().getWriteCapacityUnits();

        ProvisionedThroughput throughput = new ProvisionedThroughput();
        Optional<Long> readUnits = (isRaise) ? task.getReadCapacityUnits().get().getRaise() : task.getReadCapacityUnits().get().getNormal();
        if (readUnits.isPresent()) {
            Long readUnitsLong = readUnits.get();
            if (currentReadCapacityUnit != readUnitsLong) {
                throughput.withReadCapacityUnits(readUnitsLong);
                isNeedChange = true;
            }
        }
        Optional<Long> writeUnits = (isRaise) ? task.getWriteCapacityUnits().get().getRaise() : task.getWriteCapacityUnits().get().getNormal();
        if (writeUnits.isPresent()) {
            Long writeUnitsLong = writeUnits.get();
            if (currentWriteCapacityUnit != writeUnitsLong) {
                throughput.withWriteCapacityUnits(writeUnitsLong);
                isNeedChange = true;
            }
        }

        if (isNeedChange) {
            table.updateTable(throughput);
            log.info(String.format("Updated Provisioned Throughput of table[%s]. read_capacity_unit[%s], write_capacity_unit[%s]",
                    task.getTable(), readUnits.orNull(), writeUnits.orNull())
            );
            table.waitForActive();
        }
        else {
            log.info(String.format("No Provisioned Throughput update is needed for table[%s]. Current value is read_capacity_unit[%s], write_capacity_unit[%s]",
                    task.getTable(), currentReadCapacityUnit, currentWriteCapacityUnit)
            );
        }
    }

    // Parse like "table_%Y_%m"(include pattern or not) format using Java is difficult. So use jRuby.
    public String generateTableName(String tableName)
    {
        ScriptingContainer jruby = new ScriptingContainer();
        return jruby.runScriptlet("Time.now.strftime('" + tableName + "')").toString();
    }

    private ArrayList<KeySchemaElement> getKeySchemaElements(DynamodbOutputPlugin.PluginTask task)
    {
        ArrayList<KeySchemaElement> keySchema = new ArrayList<>();
        keySchema.add(new KeySchemaElement().withAttributeName(task.getPrimaryKey().get()).withKeyType(KeyType.HASH));
        if (task.getSortKey().isPresent()) {
            String sortKey = task.getSortKey().get();
            keySchema.add(new KeySchemaElement().withAttributeName(sortKey).withKeyType(KeyType.RANGE));
        }
        return keySchema;
    }

    private ArrayList<AttributeDefinition> getAttributeDefinitions(DynamodbOutputPlugin.PluginTask task)
    {
        ArrayList<AttributeDefinition> attributeDefinitions = new ArrayList<>();
        attributeDefinitions.add(
                new AttributeDefinition()
                        .withAttributeName(task.getPrimaryKey().get())
                        .withAttributeType(getAttributeType(task.getPrimaryKeyType().get())));
        if (task.getSortKey().isPresent()) {
            String sortKey = task.getSortKey().get();
            attributeDefinitions.add(
                    new AttributeDefinition()
                            .withAttributeName(sortKey)
                            .withAttributeType(getAttributeType(task.getSortKeyType().get())));
        }
        return attributeDefinitions;
    }

    private ScalarAttributeType getAttributeType(String type)
    {
        switch (type.toLowerCase()) {
            case "string":
                return ScalarAttributeType.S;
            case "number":
                return ScalarAttributeType.N;
            case "binary":
                return ScalarAttributeType.B;
            default:
                throw new UnknownScalarAttributeTypeException(type + " is invalid key type");
        }
    }

    public class ConnectionException extends RuntimeException implements UserDataException
    {
        protected ConnectionException()
        {
        }

        public ConnectionException(String message)
        {
            super(message);
        }

        public ConnectionException(Throwable cause)
        {
            super(cause);
        }
    }

    public class UnknownScalarAttributeTypeException extends RuntimeException implements UserDataException
    {
        protected UnknownScalarAttributeTypeException()
        {
        }

        public UnknownScalarAttributeTypeException(String message)
        {
            super(message);
        }

        public UnknownScalarAttributeTypeException(Throwable cause)
        {
            super(cause);
        }
    }
}
