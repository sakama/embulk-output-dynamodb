package org.embulk.output.dynamodb;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableWriteItems;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.DataException;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.msgpack.value.BooleanValue;
import org.msgpack.value.FloatValue;
import org.msgpack.value.IntegerValue;
import org.msgpack.value.Value;
import org.slf4j.Logger;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

public class DynamodbOutputPlugin
        implements OutputPlugin
{
    public interface CapacityTask
        extends Task
    {
        @Config("normal")
        @ConfigDefault("null")
        Optional<Long> getNormal();

        @Config("raise")
        @ConfigDefault("null")
        Optional<Long> getRaise();
    }

    public interface PluginTask
            extends AwsCredentialsTask, Task
    {
        @Config("mode")
        @ConfigDefault("\"upsert\"")
        Mode getMode();

        @Config("region")
        String getRegion();

        @Config("auto_create_table")
        @ConfigDefault("false")
        Boolean getAutoCreateTable();

        @Config("table")
        String getTable();
        void setTable(String table);

        @Config("update_expression")
        @ConfigDefault("null")
        Optional<String> getUpdateExpression();

        @Config("write_capacity_units")
        @ConfigDefault("null")
        Optional<CapacityTask> getWriteCapacityUnits();

        @Config("read_capacity_units")
        @ConfigDefault("null")
        Optional<CapacityTask> getReadCapacityUnits();

        @Config("max_put_items")
        @ConfigDefault("25")
        int getMaxPutItems();

        @Config("endpoint")
        @ConfigDefault("null")
        Optional<String> getEndpoint();

        @Config("primary_key")
        @ConfigDefault("null")
        Optional<String> getPrimaryKey();

        @Config("primary_key_type")
        @ConfigDefault("null")
        Optional<String> getPrimaryKeyType();

        @Config("sort_key")
        @ConfigDefault("null")
        Optional<String> getSortKey();

        @Config("sort_key_type")
        @ConfigDefault("null")
        Optional<String> getSortKeyType();
    }

    private final Logger log;
    private final DynamodbUtils dynamoDbUtils;

    @Inject
    public DynamodbOutputPlugin()
    {
        log = Exec.getLogger(getClass());
        dynamoDbUtils = new DynamodbUtils();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);
        dynamoDbUtils.configCheck(task);

        DynamoDB dynamoDB = null;
        try {
            dynamoDB = dynamoDbUtils.createDynamoDB(task);
            log.info(String.format("Executing plugin with '%s' mode", task.getMode()));
            task.setTable(dynamoDbUtils.generateTableName(task.getTable()));
            if (task.getAutoCreateTable()) {
                dynamoDbUtils.createTable(dynamoDB, task);
            }
            // Up to raised provisioned value
            dynamoDbUtils.updateTableProvision(dynamoDB, task, true);

            control.run(task.dump());

            // Back to normal provisioned value
            dynamoDbUtils.updateTableProvision(dynamoDB, task, false);
        }
        catch (AmazonClientException | InterruptedException ex) {
            throw Throwables.propagate(ex);
        }
        finally {
            if (dynamoDB != null) {
                dynamoDB.shutdown();
            }
        }
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        // TODO
        return Exec.newConfigDiff();
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)
    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);
        DynamodbPageOutput pageOutput = null;
        try {
            DynamoDB dynamoDB = dynamoDbUtils.createDynamoDB(task);
            pageOutput = new DynamodbPageOutput(task, dynamoDB);
            pageOutput.open(schema);
        }
        catch (AmazonClientException ex) {
            Throwables.propagate(ex);
        }
        return pageOutput;
    }

    public static class DynamodbPageOutput implements TransactionalPageOutput
    {
        private Logger log;
        private DynamodbUtils dynamodbUtils;
        private DynamoDB dynamoDB;
        private PageReader pageReader;
        private int totalWroteItemSize = 0;
        private int currentBufferItemSize = 0;
        private TableWriteItems items;

        private final String table;
        private final Mode mode;
        private final Optional<String> updateExpression;
        private final List primaryKeyElements;
        private final int maxPutItems;

        public DynamodbPageOutput(PluginTask task, DynamoDB dynamoDB)
        {
            this.log = Exec.getLogger(getClass());
            this.dynamodbUtils = new DynamodbUtils();
            this.dynamoDB = dynamoDB;
            this.table = task.getTable();
            this.mode = task.getMode();
            this.updateExpression = task.getUpdateExpression();
            this.primaryKeyElements = (mode.equals(Mode.UPSERT_WITH_EXPRESSION)) ? dynamodbUtils.getPrimaryKey(dynamoDB, table) : null;
            this.maxPutItems = task.getMaxPutItems();
        }

        void open(final Schema schema)
        {
            pageReader = new PageReader(schema);
            if (mode.equals(Mode.UPSERT)) {
                items = new TableWriteItems(table);
            }
        }

        @Override
        public void add(Page page)
        {
            pageReader.setPage(page);
            while (pageReader.nextRecord()) {
                try {
                    final Item item = new Item();

                    pageReader.getSchema().visitColumns(new ColumnVisitor() {
                        @Override
                        public void booleanColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                item.withBoolean(column.getName(), pageReader.getBoolean(column));
                            }
                        }

                        @Override
                        public void longColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                item.withLong(column.getName(), pageReader.getLong(column));
                            }
                        }

                        @Override
                        public void doubleColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                item.withDouble(column.getName(), pageReader.getDouble(column));
                            }
                        }

                        @Override
                        public void stringColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                item.withString(column.getName(), pageReader.getString(column));
                            }
                        }

                        @Override
                        public void timestampColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                item.withString(column.getName(), String.valueOf(pageReader.getTimestamp(column)));
                            }
                        }

                        @Override
                        public void jsonColumn(Column column)
                        {
                            if (pageReader.isNull(column)) {
                                addNullValue(column.getName());
                            }
                            else {
                                Value jsonValue = pageReader.getJson(column);
                                if (jsonValue.isArrayValue()) {
                                    List<Object> list = new ArrayList<Object>();
                                    for (Value v : jsonValue.asArrayValue()) {
                                        list.add(getRawFromValue(v));
                                    }
                                    item.withList(column.getName(), list);
                                }
                                else {
                                    item.withJSON(column.getName(), jsonValue.toJson());
                                }
                            }
                        }

                        private void addNullValue(String name)
                        {
                            item.withNull(name);
                        }

                        private Object getRawFromValue(Value value)
                        {
                            if (value.isBooleanValue()) {
                                return ((BooleanValue) value).getBoolean();
                            }
                            else if (value.isStringValue()) {
                                return value.toString();
                            }
                            else if (value.isIntegerValue()) {
                                return ((IntegerValue) value).asLong();
                            }
                            else if (value.isFloatValue()) {
                                return ((FloatValue) value).toDouble();
                            }
                            else if (value.isArrayValue()) {
                                List<Object> list = new ArrayList<>();
                                for (Value v : value.asArrayValue()) {
                                    list.add(getRawFromValue(v));
                                }
                                return list;
                            }
                            else if (value.isMapValue()) {
                                Map<String, Object> map = new LinkedHashMap<>();
                                for (Map.Entry<Value, Value> entry : value.asMapValue().entrySet()) {
                                    map.put(entry.getKey().toString(), getRawFromValue(entry.getValue()));
                                }
                                return map;
                            }
                            else if (value.isNilValue()) {
                                return null;
                            }
                            throw new DataException("Record has invalid json column value");
                        }
                    });

                    if (mode.equals(Mode.UPSERT)) {
                        addItemToBuffer(item);
                    }
                    else if (mode.equals(Mode.UPSERT_WITH_EXPRESSION)) {
                        updateItem(item);
                    }
                }
                catch (AmazonServiceException ex) {
                    throw Throwables.propagate(ex);
                }
            }
        }

        // upsert mode only
        public void addItemToBuffer(Item item)
        {
            items.addItemToPut(item);
            currentBufferItemSize++;
            totalWroteItemSize++;
            // @see http://docs.aws.amazon.com/amazondynamodb/latest/APIReference/API_BatchWriteItem.html
            if (currentBufferItemSize >= maxPutItems) {
                flush();
            }
        }

        // upsert mode only
        public void flush()
        {
            if (currentBufferItemSize > 0) {
                try {
                    dynamodbUtils.batchWriteItem(dynamoDB, items);
                    if (totalWroteItemSize % 1000 == 0) {
                        log.info(String.format("Wrote %s items", totalWroteItemSize));
                    }
                }
                catch (AmazonServiceException ex) {
                    if (ex.getErrorCode().equals("ValidationException")) {
                        log.error(String.format("Data was invalid. data:%s", items.getItemsToPut()));
                    }
                    throw Throwables.propagate(ex);
                }
                finally {
                    // Re-initialize for next loop
                    items = new TableWriteItems(table);
                    currentBufferItemSize = 0;
                }
            }
        }

        // upsert_with_expression mode only
        public void updateItem(Item item)
        {
            try {
                dynamodbUtils.updateItem(dynamoDB, table, item, primaryKeyElements, updateExpression);
                totalWroteItemSize++;
                if (totalWroteItemSize % 1000 == 0) {
                    log.info(String.format("Updated %s items", totalWroteItemSize));
                }
            }
            catch (AmazonServiceException ex) {
                if (ex.getErrorCode().equals("ValidationException")) {
                    log.error(String.format("Data was invalid. data:%s", items.getItemsToPut()));
                }
                throw Throwables.propagate(ex);
            }
        }

        @Override
        public void finish()
        {
            close();
            log.info(String.format("Completed to write total %s items", totalWroteItemSize));
        }

        @Override
        public void close()
        {
            if (mode.equals(Mode.UPSERT)) {
                flush();
            }
            if (dynamoDB != null) {
                dynamoDB.shutdown();
                dynamoDB = null;
            }
        }

        @Override
        public void abort()
        {
            // nothing
        }

        @Override
        public TaskReport commit()
        {
            return Exec.newTaskReport();
        }
    }

    public enum Mode
    {
        UPSERT,
        UPSERT_WITH_EXPRESSION;

        @JsonValue
        @Override
        public String toString()
        {
            return name().toLowerCase(Locale.ENGLISH);
        }

        @JsonCreator
        public static Mode fromString(String value)
        {
            switch (value) {
                case "upsert":
                    return UPSERT;
                case "upsert_with_expression":
                    return UPSERT_WITH_EXPRESSION;
                default:
                    throw new ConfigException(String.format("Unknown mode '%s'. Supported modes are upsert and upsert_with_expression", value));
            }
        }
    }
}
