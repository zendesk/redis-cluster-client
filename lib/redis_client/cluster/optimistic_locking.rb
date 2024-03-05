# frozen_string_literal: true

require 'redis_client'
require 'redis_client/cluster/transaction'

class RedisClient
  class Cluster
    class OptimisticLocking
      def initialize(router, command_builder)
        @router = router
        @command_builder = command_builder
        @slot = nil
        @conn = nil
        @asking = false
      end

      def watch(keys, &block)
        if @conn
          # We're already watching, and the caller wants to watch additional keys
          add_to_watch(keys)
        else
          # First call to #watch
          start_watch(keys, &block)
        end
      end

      def unwatch
        @conn.call('UNWATCH')
      end

      def multi
        transaction = ::RedisClient::Cluster::Transaction.new(
          @router, @command_builder, node: @conn, slot: @slot, asking: @asking
        )
        yield transaction
        transaction.execute
      end

      private

      def start_watch(keys)
        @slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "unsafe watch: #{keys.join(' ')}" if @slot.nil?

        # We have not yet selected a node for this transaction, initially, which means we can handle
        # redirections freely initially (i.e. for the first WATCH call)
        handle_redirection(retry_count: 1) do |nd|
          nd.with do |c|
            c.ensure_connected_cluster_scoped(retryable: false) do
              @conn = c
              @conn.call('ASKING') if @asking
              @conn.call('WATCH', *keys)
              begin
                yield(c, @slot, @asking)
              rescue ::RedisClient::ConnectionError
                # No need to unwatch on a connection error.
                raise
              rescue StandardError
                unwatch
                raise
              end
            end
          end
        end
      end

      def add_to_watch(keys)
        slot = find_slot(keys)
        raise ::RedisClient::Cluster::Transaction::ConsistencyError, "inconsistent watch: #{keys.join(' ')}" if slot != @slot

        @conn.call('WATCH', *keys)
      end

      def handle_redirection(retry_count: 1, &blk)
        node = @router.find_primary_node_by_slot(@slot)
        times_block_executed = 0
        @router.handle_redirection(node, nil, retry_count: retry_count) do |nd|
          times_block_executed = 1
          handle_asking_once(nd, &blk)
        end
      rescue ::RedisClient::ConnectionError
        # Deduct the number of retries that happened _inside_ router#handle_redirection from our remaining
        # _external_ retries. Always deduct at least one in case handle_redirection raises without trying the block.
        retry_count -= [times_block_executed, 1].min
        raise if retry_count < 0

        retry
      end

      def handle_asking_once(node)
        yield node
      rescue ::RedisClient::CommandError => e
        raise unless ErrorIdentification.client_owns_error?(e, node)
        raise unless e.message.start_with?('ASK')

        node = @router.assign_asking_node(e.message)
        @asking = true
        yield node
      ensure
        @asking = false
      end

      def find_slot(keys)
        return if keys.empty?
        return if keys.any? { |k| k.nil? || k.empty? }

        slots = keys.map { |k| @router.find_slot_by_key(k) }
        return if slots.uniq.size != 1

        slots.first
      end
    end
  end
end
