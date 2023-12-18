# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module TestingTopologyMixin
        def setup
          @test_config = ::RedisClient::ClusterConfig.new(
            nodes: TEST_NODE_URIS,
            fixed_hostname: TEST_FIXED_HOSTNAME,
            **self.class::TESTING_TOPOLOGY_OPTIONS,
            **TEST_GENERIC_OPTIONS
          )
          @concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create
          @test_node = ::RedisClient::Cluster::Node.new(@concurrent_worker, config: @test_config)
          @replications = @test_node.instance_variable_get(:@replications)
          @test_topology = @test_node.instance_variable_get(:@topology)
        end

        def teardown
          @test_node&.each(&:close)
        end
      end
    end
  end
end
