require "ostruct"
require "cassandra"

module Curator
  module Cassandra
    class DataStore
      def client
        return @client if @client
        return @client = Curator.config.client if Curator.config.client

        yml_config = YAML.load(File.read(Curator.config.cassandra_config_file))[Curator.config.environment]
        cluster = ::Cassandra.connect(yml_config)
        @keyspace_with_environment = Curator.config.keyspace + "_" + Curator.config.environment

        @client = cluster.connect
        @client.execute(<<-END)
        CREATE KEYSPACE IF NOT EXISTS #{@keyspace_with_environment} WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3};
        END
        @client.execute("USE #{@keyspace_with_environment}")
        @client
      end

      def settings(collection_name)
        raise StandardError, "Not implemented yet"
      end

      def update_settings!(collection_name, updated_settings)
        raise StandardError, "Not implemented yet"
      end

      def save(options)
        columns = options[:value].keys.map(&:to_s)
        question_marks = options[:value].size.times.map { "?" }.join(",")
        statement = @client.prepare("INSERT INTO #{options[:collection_name]} (#{columns}) VALUES (#{question_marks})")
        statement.execute(*options[:value].values)
      end

      def reset!
        @client.keyspace(@keyspace_with_environment).each_table do |table|
          @client.execute("TRUNCATE #{table.name}")
        end
      end
    end
  end
end
