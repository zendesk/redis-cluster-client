# frozen_string_literal: true

class RedisClient
  class Cluster
    class Node
      module ReplicaMixin
        attr_reader :clients, :primary_clients

        EMPTY_ARRAY = [].freeze

        def initialize(replications, options, pool, _concurrent_worker, **kwargs)
          @pool = pool
          @clients = {}
          @client_options = kwargs.reject { |k, _| ::RedisClient::Cluster::Node::IGNORE_GENERIC_CONFIG_KEYS.include?(k) }
          process_topology_update!(replications, options)
        end

        def any_primary_node_key(seed: nil)
          random = seed.nil? ? Random : Random.new(seed)
          @primary_node_keys.sample(random: random)
        end

        def process_topology_update!(replications, options) # rubocop:disable Metrics/AbcSize
          @replications = replications
          @primary_node_keys = @replications.keys.sort
          @replica_node_keys = @replications.values.flatten.sort

          # Disconnect from nodes that we no longer want
          (@clients.keys - options.keys).each do |node_key|
            @clients.delete(node_key).close
          end

          # Connect to nodes that we are not yet connected to
          (options.keys - @clients.keys).each do |node_key|
            option = options[node_key].merge(@client_options)
            config = ::RedisClient::Cluster::Node::Config.new(scale_read: !@primary_node_keys.include?(node_key), **option)
            client = @pool.nil? ? config.new_client : config.new_pool(**@pool)
            @clients[node_key] = client
          end

          @primary_clients = @clients.select { |k, _| @primary_node_keys.include?(k) }
        end
      end
    end
  end
end
