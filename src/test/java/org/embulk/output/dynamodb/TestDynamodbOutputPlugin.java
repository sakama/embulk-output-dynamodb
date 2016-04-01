package org.embulk.output.dynamodb;

import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.embulk.EmbulkTestRuntime;
import org.embulk.config.ConfigException;
import org.embulk.config.ConfigSource;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.output.dynamodb.DynamodbOutputPlugin.PluginTask;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageTestUtils;
import org.embulk.spi.Schema;
import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.standards.CsvParserPlugin;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class TestDynamodbOutputPlugin
{
    private static String PATH_PREFIX;

    private MockPageOutput pageOutput;

    @BeforeClass
    public static void initializeConstant()
    {
        PATH_PREFIX = DynamodbOutputPlugin.class.getClassLoader().getResource("sample_01.csv").getPath();
    }

    @Rule
    public EmbulkTestRuntime runtime = new EmbulkTestRuntime();
    private DynamodbOutputPlugin plugin;

    @Before
    public void createResources() throws Exception
    {
        ConfigSource config = config();
        plugin = new DynamodbOutputPlugin();
        PluginTask task = config.loadConfig(PluginTask.class);
        pageOutput = new MockPageOutput();

        DynamodbUtils dynamoDbUtils = new DynamodbUtils();
        DynamoDB dynamoDB = null;
        try {
            dynamoDB = dynamoDbUtils.createDynamoDB(task);
            if (dynamoDbUtils.isExistsTable(dynamoDB, task.getTable())) {
                dynamoDbUtils.deleteTable(dynamoDB, task.getTable());
            }
            dynamoDbUtils.createTable(dynamoDB, task);
        }
        finally {
            if (dynamoDB != null) {
                dynamoDB.shutdown();
            }
        }
    }

    @Test
    public void testDefaultValues()
    {
        ConfigSource config = config();
        DynamodbOutputPlugin.PluginTask task = config.loadConfig(PluginTask.class);
        assertEquals("us-west-1", task.getRegion());
    }

    @Test
    public void testTransaction()
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        // no error happens
    }

    @Test
    public void testResume()
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.resume(task.dump(), schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
    }

    @Test
    public void testCleanup()
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.cleanup(task.dump(), schema, 0, Arrays.asList(Exec.newTaskReport()));
        // no error happens
    }

    @Test
    public void testOutputByOpen() throws Exception
    {
        ConfigSource config = config();
        Schema schema = config.getNested("parser").loadConfig(CsvParserPlugin.PluginTask.class).getSchemaConfig().toSchema();
        PluginTask task = config.loadConfig(PluginTask.class);
        plugin.transaction(config, schema, 0, new OutputPlugin.Control()
        {
            @Override
            public List<TaskReport> run(TaskSource taskSource)
            {
                return Lists.newArrayList(Exec.newTaskReport());
            }
        });
        TransactionalPageOutput output = plugin.open(task.dump(), schema, 0);

        List<Page> pages = PageTestUtils.buildPage(runtime.getBufferAllocator(), schema, 1L, 32864L, "2015-01-27T19:23:49", "2015-01-27T00:00:00",  true, 123.45, "embulk");
        assertEquals(1, pages.size());
        for (Page page : pages) {
            output.add(page);
        }

//        output.finish();
//        output.commit();
//
//        DynamodbUtils dynamoDbUtils = new DynamodbUtils();
//        DynamoDB dynamoDB = null;
//        try {
//            dynamoDB = dynamoDbUtils.createDynamoDB(task);
//
//            Table table = dynamoDB.getTable(task.getTable());
//            ItemCollection<ScanOutcome> items = table.scan();
//
//            while (items.iterator().hasNext()) {
//                Map<String, Object> item = items.iterator().next().asMap();
//                assertEquals(1, item.get("id"));
//                assertEquals(32864, item.get("account"));
//                assertEquals("2015-01-27T19:23:49", item.get("time"));
//                assertEquals("2015-01-27T00:00:00", item.get("purchase"));
//                assertEquals(true, item.get("flg"));
//                assertEquals(123.45, item.get("score"));
//                assertEquals("embulk", item.get("comment"));
//            }
//        }
//        finally {
//            if (dynamoDB != null) {
//                dynamoDB.shutdown();
//            }
//        }
    }

    @Test
    public void testMode()
    {
        assertEquals(2, DynamodbOutputPlugin.Mode.values().length);
        assertEquals(DynamodbOutputPlugin.Mode.UPSERT, DynamodbOutputPlugin.Mode.valueOf("UPSERT"));
    }

    @Test(expected = ConfigException.class)
    public void testModeThrowsConfigException()
    {
        DynamodbOutputPlugin.Mode.fromString("non-exists-mode");
    }

    private ConfigSource config()
    {
        return Exec.newConfigSource()
                .set("in", inputConfig())
                .set("parser", parserConfig(schemaConfig()))
                .set("type", "dynamodb")
                .set("mode", "upsert")
                .set("region", "us-west-1")
                .set("table", "dummy")
                .set("primary_key", "id")
                .set("primary_key_type", "string")
                .set("read_capacity_units", capacityUnitConfig())
                .set("write_capacity_units", capacityUnitConfig())
                .set("auth_method", "basic")
                .set("access_key_id", "dummy")
                .set("secret_access_key", "dummy")
                .set("endpoint", "http://localhost:8000");
    }

    private ImmutableMap<String, Object> capacityUnitConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("normal", 5L);
        builder.put("raise", 8L);
        return builder.build();
    }

    private ImmutableMap<String, Object> inputConfig()
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "file");
        builder.put("path_prefix", PATH_PREFIX);
        builder.put("last_path", "");
        return builder.build();
    }

    private ImmutableMap<String, Object> parserConfig(ImmutableList<Object> schemaConfig)
    {
        ImmutableMap.Builder<String, Object> builder = new ImmutableMap.Builder<>();
        builder.put("type", "csv");
        builder.put("newline", "CRLF");
        builder.put("delimiter", ",");
        builder.put("quote", "\"");
        builder.put("escape", "\"");
        builder.put("trim_if_not_quoted", false);
        builder.put("skip_header_lines", 1);
        builder.put("allow_extra_columns", false);
        builder.put("allow_optional_columns", false);
        builder.put("columns", schemaConfig);
        return builder.build();
    }

    private ImmutableList<Object> schemaConfig()
    {
        ImmutableList.Builder<Object> builder = new ImmutableList.Builder<>();
        builder.add(ImmutableMap.of("name", "id", "type", "long"));
        builder.add(ImmutableMap.of("name", "account", "type", "long"));
        builder.add(ImmutableMap.of("name", "time", "type", "timestamp", "format", "%Y-%m-%d %H:%M:%S"));
        builder.add(ImmutableMap.of("name", "purchase", "type", "timestamp", "format", "%Y%m%d"));
        builder.add(ImmutableMap.of("name", "flg", "type", "boolean"));
        builder.add(ImmutableMap.of("name", "score", "type", "double"));
        builder.add(ImmutableMap.of("name", "comment", "type", "string"));
        return builder.build();
    }
}
