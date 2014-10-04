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

      def delete(table_name, key)
        _execute("DELETE FROM #{table_name} WHERE key = ?", key)
      end

      def find_all(table_name)
        _execute("SELECT * FROM #{table_name}").rows.map(&:with_indifferent_access)
      end

      def find_by_attribute(table_name, column, value)
        _execute("SELECT * FROM #{table_name} WHERE #{column} = ?", value).rows.map(&:with_indifferent_access)
      end

      def find_by_key(table_name, key)
        _execute("SELECT * FROM #{table_name}").first
      end

      def save(options)
        _create_table_if_needed(options)

        columns = options[:value].keys.map(&:to_s).join(",")
        question_marks = options[:value].size.times.map { "?" }.join(",")
        _execute("INSERT INTO #{options[:collection_name]} (key, #{columns}) VALUES (?, #{question_marks})", *options[:value].values.unshift(options[:key]))
      end

      def reset!
        if _keyspace
          _keyspace.each_table do |table|
            _execute("DROP TABLE #{table.name}")
          end
        end
      end

      def _create_table_if_needed(options)
        table_name = options[:collection_name]

        return if _keyspace && _keyspace.tables.map(&:name).include?(table_name)

        columns = options[:value].keys.map { |key| "#{key} #{_cql_type(options[:value][key])}" }.join(",")

        _execute(<<-CQL)
          CREATE TABLE #{table_name} (
            key text PRIMARY KEY,
            #{columns}
          )
        CQL

        (options[:index] || {}).keys.each do |indexed_column|
          _execute("CREATE INDEX ON #{table_name} (#{indexed_column})")
        end
      end

      def _execute(*args)
        _session.execute(*args)
      end

      def _keyspace
        @keyspace = client.keyspace(_keyspace_name)
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
