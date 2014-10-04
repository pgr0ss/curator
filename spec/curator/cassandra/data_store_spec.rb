require 'spec_helper'
require 'active_support/core_ext/numeric/time'
require 'active_support/core_ext/date/calculations'
require 'curator/cassandra/data_store'
require 'curator/shared_data_store_specs'

module Curator
  module Cassandra
    describe Curator::Cassandra::DataStore do
      include_examples "data_store"

      let(:data_store) { DataStore.new }

      with_config do
        Curator.configure(:cassandra) do |config|
          config.environment = "test"
          config.keyspace = "curator"
          config.cassandra_config_file = "config/cassandra.yml"
        end
      end

      describe "self.client" do
        context "with a client manually configured" do
          with_config do
            Curator.configure(:cassandra) do |config|
              config.environment = "test"
              config.keyspace = "curator"
              config.client = ::Cassandra.connect
            end
          end

          it "should return the client and not use the yaml file" do
            data_store.client.should == Curator.config.client
          end
        end

        it "returns a cassandra client with a config read from the yml file provided" do
          begin
            File.stub(:read).and_return(<<-YML)
            test:
              :hosts:
                - 127.0.0.1
              :port: 9042
            YML
            data_store.instance_variable_set('@client', nil)
            data_store.client.should_not be_nil
          ensure
            data_store.instance_variable_set("@client", nil)
          end
        end
      end
    end
  end
end
