require "ostruct"
require "cassandra"

module Curator
  module Cassandra
    class DataStore
      def client
        return @client if @client
        return @client = Curator.config.client if Curator.config.client

        yml_config = YAML.load(File.read(Curator.config.cassandra_config_file))[Curator.config.environment]
        @client = ::Cassandra.connect(yml_config)
        @client
      end

      def settings(collection_name)
        raise StandardError, "Not implemented yet"
      end

      def update_settings!(collection_name, updated_settings)
        raise StandardError, "Not implemented yet"
      end

      def save(options)
        _create_table_if_needed(options)

        columns = options[:value].keys.map(&:to_s).join(",")
        question_marks = options[:value].size.times.map { "?" }.join(",")
        statement = _session.prepare("INSERT INTO #{options[:collection_name]} (key, #{columns}) VALUES (?, #{question_marks})")
        _session.execute(statement, *options[:value].values.unshift(options[:key]))
      end

      def reset!
        if _keyspace
          _keyspace.each_table do |table|
            _session.execute("DROP TABLE #{table.name}")
          end
        end
      end

      def _create_table_if_needed(options)
        table_name = options[:collection_name]
        return if _keyspace && _keyspace.tables.map(&:name).include?(table_name)

        columns = options[:value].keys.map { |key| "#{key} #{_cql_type(options[:value][key])}" }.join(",")

        _session.execute(<<-CQL)
          CREATE TABLE #{table_name} (
            key text PRIMARY KEY,
            #{columns}
          )
        CQL
      end

      def _keyspace
        @keyspace ||= client.keyspace(_keyspace_name)
      end

      def _keyspace_name
        Curator.config.keyspace + "_" + Curator.config.environment
      end

      def _session
        return @session if @session

        @session = client.connect
        @session.execute(<<-END)
        CREATE KEYSPACE IF NOT EXISTS #{_keyspace_name} WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':3};
        END
        @session.execute("USE #{_keyspace_name}")
        @session
      end

      def _cql_type(obj)
        case obj
        when Integer then "bigint"
        else "text"
        end
      end

    end
  end
end
