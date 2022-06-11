# frozen_string_literal: true

require 'redis_client'
require 'redis_client/config'
require 'redis_client/cluster/errors'

class RedisClient
  class Cluster
    class Node
      include Enumerable

      SLOT_SIZE = 16_384
      ReloadNeeded = Class.new(::RedisClient::Error)

      class Config < ::RedisClient::Config
        def initialize(scale_read: false, **kwargs)
          @scale_read = scale_read
          super(**kwargs)
        end

        def build_connection_prelude
          prelude = super.dup
          prelude << ['READONLY'] if @scale_read
          prelude.freeze
        end
      end

      class << self
        def load_info(options, **kwargs)
          tmp_nodes = ::RedisClient::Cluster::Node.new(options, **kwargs)

          errors = tmp_nodes.map do |tmp_node|
            reply = tmp_node.call('CLUSTER', 'NODES')
            return parse_node_info(reply)
          rescue ::RedisClient::ConnectionError, ::RedisClient::CommandError => e
            e
          end

          raise ::RedisClient::Cluster::InitialSetupError, errors
        ensure
          tmp_nodes&.each(&:close)
        end

        private

        # @see https://redis.io/commands/cluster-nodes/
        def parse_node_info(info) # rubocop:disable Metrics/AbcSize, Metrics/CyclomaticComplexity, Metrics/PerceivedComplexity, Metrics/MethodLength
          rows = info.split("\n").map(&:split)
          rows.each { |arr| arr[2] = arr[2].split(',') }
          rows.select! { |arr| arr[7] == 'connected' && (arr[2] & %w[fail? fail handshake noaddr noflags]).empty? }
          rows.each do |arr|
            arr[1] = arr[1].split('@').first
            arr[2] = (arr[2] & %w[master slave]).first
            arr[8] = arr[8].nil? ? [] : arr[8].split(',').map { |r| r.split('-').map { |s| Integer(s) } }
          end

          rows.map do |arr|
            { id: arr[0], node_key: arr[1], role: arr[2], primary_id: arr[3], ping_sent: arr[4],
              pong_recv: arr[5], config_epoch: arr[6], link_state: arr[7], slots: arr[8] }
          end
        end
      end

      def initialize(options, node_info = [], pool = nil, with_replica: false, **kwargs)
        @with_replica = with_replica
        @slots = build_slot_node_mappings(node_info)
        @replications = build_replication_mappings(node_info)
        @clients = build_clients(options, pool, **kwargs)
      end

      def inspect
        @clients.keys.sort.to_s
      end

      def each(&block)
        @clients.values.each(&block)
      end

      def sample
        @clients.values.sample
      end

      def find_by(node_key)
        @clients.fetch(node_key)
      rescue KeyError
        raise ReloadNeeded
      end

      def call_all(method, *command, **kwargs, &block)
        try_map { |_, client| client.send(method, *command, **kwargs, &block) }.values
      end

      def call_primary(method, *command, **kwargs, &block)
        try_map do |node_key, client|
          next if replica?(node_key)

          client.send(method, *command, **kwargs, &block)
        end.values
      end

      def call_replica(method, *command, **kwargs, &block)
        return call_primary(method, *command, **kwargs, &block) if replica_disabled?

        try_map do |node_key, client|
          next if primary?(node_key)

          client.send(method, *command, **kwargs, &block)
        end.values
      end

      # TODO: impl
      def process_all(commands, &block)
        try_map { |_, client| client.process(commands, &block) }.values
      end

      def scale_reading_clients
        reading_clients = []

        @clients.each do |node_key, client|
          next unless replica_disabled? ? primary?(node_key) : replica?(node_key)

          reading_clients << client
        end

        reading_clients
      end

      def slot_exists?(slot)
        !@slots[slot].nil?
      end

      def find_node_key_of_primary(slot)
        @slots[slot]
      end

      def find_node_key_of_replica(slot)
        return @slots[slot] if replica_disabled? || @replications[@slots[slot]].size.zero?

        @replications[@slots[slot]].sample
      end

      def update_slot(slot, node_key)
        @slots[slot] = node_key
      end

      private

      def replica_disabled?
        !@with_replica
      end

      def primary?(node_key)
        !replica?(node_key)
      end

      def replica?(node_key)
        !(@replications.nil? || @replications.size.zero?) && @replications[node_key].size.zero?
      end

      def build_slot_node_mappings(node_info)
        slots = Array.new(SLOT_SIZE)
        node_info.each do |info|
          next if info[:slots].nil? || info[:slots].empty?

          info[:slots].each { |start, last| (start..last).each { |i| slots[i] = info[:node_key] } }
        end

        slots
      end

      def build_replication_mappings(node_info)
        dict = node_info.to_h { |info| [info[:id], info] }
        node_info.each_with_object(Hash.new { |h, k| h[k] = [] }) do |info, acc|
          primary_info = dict[info[:primary_id]]
          acc[primary_info[:node_key]] << info[:node_key] unless primary_info.nil?
          acc[info[:node_key]]
        end
      end

      def build_clients(options, pool, **kwargs)
        options.filter_map do |node_key, option|
          next if replica_disabled? && replica?(node_key)

          config = ::RedisClient::Cluster::Node::Config.new(scale_read: replica?(node_key), **option)
          client = pool.nil? ? config.new_client(**kwargs) : config.new_pool(**pool, **kwargs)

          [node_key, client]
        end.to_h
      end

      def try_map # rubocop:disable Metrics/MethodLength
        errors = {}
        results = {}

        @clients.each do |node_key, client|
          reply = yield(node_key, client)
          results[node_key] = reply unless reply.nil?
        rescue ::RedisClient::CommandError => e
          errors[node_key] = e
          next
        end

        return results if errors.empty?

        raise ::RedisClient::Cluster::CommandErrorCollection, errors
      end
    end
  end
end