# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module TestingTopologyMixin
        def setup
          test_config = ::RedisClient::ClusterConfig.new(
            nodes: TEST_NODE_URIS,
            fixed_hostname: TEST_FIXED_HOSTNAME,
            **TEST_GENERIC_OPTIONS
          )
          @concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create
          if TEST_FIXED_HOSTNAME
            test_node_info_list = ::RedisClient::Cluster::Node.new(@concurrent_worker, config: test_config).node_info
            test_node_info_list.each do |info|
              _, port = ::RedisClient::Cluster::NodeKey.split(info.node_key)
              info.node_key = ::RedisClient::Cluster::NodeKey.build_from_host_port(TEST_FIXED_HOSTNAME, port)
            end
            node_addrs = test_node_info_list.map { |info| ::RedisClient::Cluster::NodeKey.hashify(info.node_key) }
            test_config = ::RedisClient::ClusterConfig.new(
              nodes: node_addrs,
              fixed_hostname: TEST_FIXED_HOSTNAME,
              **TEST_GENERIC_OPTIONS
            )
          end
          test_node = ::RedisClient::Cluster::Node.new(@concurrent_worker, config: test_config)
          @options = test_node.instance_variable_get(:@node_configs)
          @replications = test_node.instance_variable_get(:@replications)
          @test_topology = test_node.instance_variable_get(:@topology)
        end

        def teardown
          @test_topology&.clients&.each_value(&:close)
        end
      end
    end
  end
end
