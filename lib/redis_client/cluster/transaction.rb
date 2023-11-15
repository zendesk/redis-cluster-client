# frozen_string_literal: true

require 'redis_client'

class RedisClient
  class Cluster
    class Transaction
      ConsistencyError = Class.new(::RedisClient::Error)
      STARTING_COMMANDS = %w[watch multi]
      COMPLETING_COMMANDS = %w[exec discard unwatch].freeze

      def self.command_starts_transaction?(command)
        STARTING_COMMANDS.include?(::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command))
      end

      def initialize(router, command_builder, first_command: nil)
        @router = router
        @command_builder = command_builder
        @node_key = nil
        @node = nil
        @pool = nil
        # @was_disable_reconection = nil
        @complete = false
        ensure_same_node(first_command, nil_ok: true) if first_command
      end

      attr_reader :node

      def send_command(method, command, *args, &block)
        # Force-disable retries in transactions
        method = :call_once if method == :call
        method = :call_once_v if method == :call_v

        # redis-rb wants to do this when it probably doesn't need to.
        cmd = ::RedisClient::Cluster::NormalizedCmdName.instance.get_by_command(command)
        if complete?
          return if COMPLETING_COMMANDS.include?(cmd)
          raise ArgumentError, "Transaction is already complete"
        end

        begin
          ensure_same_node command
          ret = @router.try_send(@node, method, command, args, &block)
        rescue ::RedisClient::ConnectionError
          mark_complete # Abort transaction on connection errors
          raise
        else
          mark_complete if COMPLETING_COMMANDS.include?(cmd)
          ret
        end
      end

      def complete?
        # The router closes the node on ConnectionError.
        @complete || (@node && !@node.connected?)
      end

      def multi(watch: nil)
        send_command(:call_once_v, ['WATCH', *watch]) if watch&.any?

        begin
          command_buffer = MultiBuffer.new
          yield command_buffer

          command_buffer.commands.each { |command| ensure_same_node(command) }
          res = @router.try_delegate(@node, :pipelined) do |p|
            p.call_once_v ['MULTI']
            command_buffer.commands.each do |command|
              p.call_once_v command
            end
            p.call_once_v ['EXEC']
          end
          res.last
        rescue
          send_command(:call_once_v, ['UNWATCH']) unless complete?
          raise
        ensure
          # In any case, multi always ends the transaction.
          mark_complete
        end
      end

      def ensure_same_node(command, nil_ok: false)
        node_key = @router.find_primary_node_key(command)

        if node_key.nil? && !nil_ok
          # If ome previous command worked out what node to use, this is OK.
          raise ConsistencyError, "Client couldn't determine the node to be executed the transaction by: #{command}" unless @node
        elsif @node.nil? && node_key
          set_node node_key
        elsif @node_key != node_key
          raise ConsistencyError, "The transaction should be done for single node: #{@node_key}, #{node_key}" if node_key != @node_key
        end
        nil
      end

      def set_node(node_key)
        @node = @router.find_node(node_key)
        @node_key = node_key

        # This is a hack, but we need to check out a connection from the pool and keep it checked out until we unwatch,
        # if we're using a Pooled backend. Otherwise, we might not run commands on the same session we watched on.
        # Note that ConnectionPool keeps a reference to this connection in a threadlocal, so we don't need to actually _use_ it
        # explicitly; it'll get returned from #with like normal.
        if @node.respond_to?(:pool)
          @pool = @node.send(:pool)
          @node = @pool.checkout
        end
        # Another hack - need to disable reconnection, but we can't use the block form.
        # @was_disable_reconection = @node.instance_variable_get(:@disable_reconnection)
        # @node.instance_variable_set(:@disable_reconnection, true)
      end

      def mark_complete
        # @node.instance_variable_set(:@disable_reconnection, @was_disable_reconnection)
        @pool.checkin if @pool
        @pool = nil
        @node = nil
        @complete = true
      end

      class MultiBuffer
        def initialize
          @commands = []
        end

        def call(*command, **kwargs, &_)
          @commands << command
          nil
        end

        def call_v(command, &_)
          @commands << command
          nil
        end

        alias call_once call
        alias call_once_v call_v
        attr_accessor :commands
      end
    end
  end
end
