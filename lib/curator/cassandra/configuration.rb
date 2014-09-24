module Curator::Cassandra
  class Configuration
    include Curator::Configuration

    attr_accessor :cassandra_config_file, :keyspace

    def data_store
      Curator::Cassandra::DataStore.new
    end
  end
end
