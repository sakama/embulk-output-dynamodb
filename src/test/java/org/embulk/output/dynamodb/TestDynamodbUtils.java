package org.embulk.output.dynamodb;

import org.embulk.spi.TestPageBuilderReader.MockPageOutput;
import org.junit.BeforeClass;

import static org.junit.Assume.assumeNotNull;

public class TestDynamodbUtils
{
    private static String DYNAMO_REGION;
    private static String DYNAMO_TABLE;
    private static String PATH_PREFIX;

    private MockPageOutput pageOutput;

    @BeforeClass
    public static void initializeConstant()
    {
        DYNAMO_REGION = System.getenv("DYNAMO_REGION") != null ? System.getenv("DYNAMO_REGION") : "";
        DYNAMO_TABLE = System.getenv("DYNAMO_TABLE") != null ? System.getenv("DYNAMO_TABLE") : "";

        assumeNotNull(DYNAMO_REGION, DYNAMO_TABLE);

        PATH_PREFIX = DynamodbOutputPlugin.class.getClassLoader().getResource("sample_01.csv").getPath();
    }
}
