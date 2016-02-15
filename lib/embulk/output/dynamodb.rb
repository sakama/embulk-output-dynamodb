Embulk::JavaPlugin.register_output(
  "dynamodb", "org.embulk.output.dynamodb.DynamodbOutputPlugin",
  File.expand_path('../../../../classpath', __FILE__))
