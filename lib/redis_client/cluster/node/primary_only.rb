# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      class PrimaryOnly
        attr_reader :clients

        def initialize(replications, options, pool, _concurrent_worker, **kwargs)
          @pool = pool
          @clients = {}
          @client_options = kwargs.reject { |k, _| ::RedisClient::Cluster::Node::IGNORE_GENERIC_CONFIG_KEYS.include?(k) }
          process_topology_update!(replications, options)
        end

        alias primary_clients clients
        alias replica_clients clients

        def clients_for_scanning(seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          @clients
        end

        def find_node_key_of_replica(primary_node_key, seed: nil) # rubocop:disable Lint/UnusedMethodArgument
          primary_node_key
        end

        def any_primary_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @primary_node_keys.sample(random: random)
        end

        alias any_replica_node_key any_primary_node_key

        def process_topology_update!(replications, options) # rubocop:disable Metrics/AbcSize
          @primary_node_keys = replications.keys.sort
          desired_node_keys = if @primary_node_keys.any?
                                options.keys.select { |node_key| @primary_node_keys.include?(node_key) }
                              else
                                # During initial startup, when we don't even _know_ what is a primary, connect to all nodes.
                                options.keys
                              end

          # disconnect from nodes we no longer want
          (@clients.keys - desired_node_keys).each do |node_key|
            @clients.delete(node_key).close
          end

          # Connect to nodes we want and are not yet connected to
          (desired_node_keys - @clients.keys).each do |node_key|
            option = options[node_key].merge(@client_options)
            config = ::RedisClient::Cluster::Node::Config.new(**option)
            client = @pool.nil? ? config.new_client : config.new_pool(**@pool)
            @clients[node_key] = client
          end
        end
      end
    end
  end
end
