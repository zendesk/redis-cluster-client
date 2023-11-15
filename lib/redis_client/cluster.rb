# frozen_string_literal: true

require 'redis_client/cluster/concurrent_worker'
require 'redis_client/cluster/pipeline'
require 'redis_client/cluster/pub_sub'
require 'redis_client/cluster/router'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    ZERO_CURSOR_FOR_SCAN = '0'

    attr_reader :config

    def initialize(config, pool: nil, concurrency: nil, **kwargs)
      @config = config
      @concurrent_worker = ::RedisClient::Cluster::ConcurrentWorker.create(**(concurrency || {}))
      @router = ::RedisClient::Cluster::Router.new(config, @concurrent_worker, pool: pool, **kwargs)
      @command_builder = config.command_builder
    end

    def inspect
      "#<#{self.class.name} #{@router.node_keys.join(', ')}>"
    end

    def call(*args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      send_command(:call_v, command, &block)
    end

    def call_v(command, &block)
      command = @command_builder.generate(command)
      send_command(:call_v, command, &block)
    end

    def call_once(*args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      send_command(:call_once_v, command, &block)
    end

    def call_once_v(command, &block)
      command = @command_builder.generate(command)
      send_command(:call_once_v, command, &block)
    end

    def blocking_call(timeout, *args, **kwargs, &block)
      command = @command_builder.generate(args, kwargs)
      send_command(:blocking_call_v, command, timeout, &block)
    end

    def blocking_call_v(timeout, command, &block)
      command = @command_builder.generate(command)
      send_command(:blocking_call_v, command, timeout, &block)
    end

    def scan(*args, **kwargs, &block)
      raise ArgumentError, 'block required' unless block
      raise ::RedisClient::Cluster::Transaction::ConsistencyError, "scan is not valid inside a transaction" if @transaction

      seed = Random.new_seed
      cursor = ZERO_CURSOR_FOR_SCAN
      loop do
        cursor, keys = @router.scan('SCAN', cursor, *args, seed: seed, **kwargs)
        keys.each(&block)
        break if cursor == ZERO_CURSOR_FOR_SCAN
      end
    end

    def sscan(key, *args, **kwargs, &block)
      node = assign_node(['SSCAN', key])
      @router.try_delegate(node, :sscan, key, *args, **kwargs, &block)
    end

    def hscan(key, *args, **kwargs, &block)
      node = assign_node(['HSCAN', key])
      @router.try_delegate(node, :hscan, key, *args, **kwargs, &block)
    end

    def zscan(key, *args, **kwargs, &block)
      node = assign_node(['ZSCAN', key])
      @router.try_delegate(node, :zscan, key, *args, **kwargs, &block)
    end

    def pipelined
      seed = @config.use_replica? && @config.replica_affinity == :random ? nil : Random.new_seed
      pipeline = ::RedisClient::Cluster::Pipeline.new(@router, @command_builder, @concurrent_worker, seed: seed)
      yield pipeline
      return [] if pipeline.empty?

      pipeline.execute
    end

    def multi(watch: nil, &block)
      make_transaction
      begin
        @transaction.multi(watch: nil, &block)
      ensure
        @transaction = nil if @transaction&.complete?
      end
    end

    def pubsub
      ::RedisClient::Cluster::PubSub.new(@router, @command_builder)
    end

    def close
      @concurrent_worker.close
      @router.close
      nil
    end

    private

    def method_missing(name, *args, **kwargs, &block)
      if @router.command_exists?(name)
        args.unshift(name)
        command = @command_builder.generate(args, kwargs)
        return send_command(:call_v, command, &block)
      end

      super
    end

    def respond_to_missing?(name, include_private = false)
      return true if @router.command_exists?(name)

      super
    end

    def make_transaction(first_command = nil)
      @transaction ||= ::RedisClient::Cluster::Transaction.new(@router, @command_builder, first_command: first_command)
    end

    def sync_transaction_state(command)
      # @transaction = nil if @transaction&.complete?
      make_transaction(command) if ::RedisClient::Cluster::Transaction.command_starts_transaction?(command)
    end


    def send_command(method, command, *args, &block)
      sync_transaction_state(command)
      if @transaction
        begin
          @transaction.send_command(method, command, *args, &block)
        ensure
          @transaction = nil if @transaction&.complete?
        end
      else
        @router.send_command(method, command, *args, &block)
      end
    end

    def assign_node(command)
      @transaction ? @transaction.node : @router.assign_node(command)
    end
  end
end
