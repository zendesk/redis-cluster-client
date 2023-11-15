# frozen_string_literal: true

require 'testing_helper'

class TestAgainstClusterState < TestingWrapper
  SLOT_SIZE = 16_384

  module Mixin
    def setup
      @controller = ClusterController.new(
        TEST_NODE_URIS,
        replica_size: TEST_REPLICA_SIZE,
        **TEST_GENERIC_OPTIONS.merge(timeout: 30.0)
      )
      @controller.rebuild
      @capture_buffer = []
      @client = new_test_client
    end

    def teardown
      @controller&.close
      @client&.close
    end

    def test_the_state_of_cluster_down
      @controller.down
      assert_raises(::RedisClient::CommandError) { @client.call('SET', 'key1', 1) }
      assert_equal('fail', fetch_cluster_info('cluster_state'))
    end

    def test_the_state_of_cluster_failover
      @controller.failover
      1000.times { |i| assert_equal('OK', @client.call('SET', "key#{i}", i)) }
      wait_for_replication
      1000.times { |i| assert_equal(i.to_s, @client.call('GET', "key#{i}")) }
      assert_equal('ok', fetch_cluster_info('cluster_state'))
    end

    def test_the_state_of_cluster_resharding
      do_resharding_test do |keys|
        keys.each do |key|
          want = key
          got = @client.call('GET', key)
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      do_resharding_test do |keys|
        values = @client.pipelined do |pipeline|
          keys.each { |key| pipeline.call('GET', key) }
        end

        keys.each_with_index do |key, i|
          want = key
          got = values[i]
          assert_equal(want, got, "Case: GET: #{key}")
        end
      end
    end

    def test_does_not_abort_transaction_on_resharding
      key = 'key1'
      @client.call_v(['SET', key, 'value1'])
      slot = ::RedisClient::Cluster::KeySlotConverter.convert(key)
      @client.call_v(['WATCH', key])
      src, dest = @controller.select_resharding_target(slot)
      @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      @controller.finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)

      # This should raise and the transaction should NOT aborted now
      assert client_in_transaction?(@client)
      assert_raises(::RedisClient::CommandError, 'MOVED') { @client.call_v(['GET', key]) }
      assert client_in_transaction?(@client)
      # In normal usage, redis-rb would catch that exception and call #unwatch, since CommandError
      # does not inherit from ConnectionError.
      assert_equal('OK', @client.call_v(['UNWATCH']))
    end

    def test_does_not_abort_transaction_on_clusterdown
      key = 'key1'
      @client.call_v(['SET', key, 'value1'])
      @client.call_v(['WATCH', key])
      @controller.down
      @controller.rebuild

      # We get no indication that anything has actually gone wrong with the transaction from redis,
      # because we made no command whilst it was down.
      assert client_in_transaction?(@client)
      @client.call_v(['GET', key])

      # but, if we try and commit something, it will fail - this is a guarantee provided by redis.
      res = @client.multi do |txn|
        txn.call_v(['SET', key, 'hello'])
      end
      assert_nil(res)
    end

    private

    def wait_for_replication
      client_side_timeout = TEST_TIMEOUT_SEC + 1.0
      server_side_timeout = (TEST_TIMEOUT_SEC * 1000).to_i
      @client.blocking_call(client_side_timeout, 'WAIT', TEST_REPLICA_SIZE, server_side_timeout)
    end

    def fetch_cluster_info(key)
      @client.call('CLUSTER', 'INFO').split("\r\n").to_h { |v| v.split(':') }.fetch(key)
    end

    def do_resharding_test(number_of_keys: 1000)
      @client.pipelined { |pipeline| number_of_keys.times { |i| pipeline.call('SET', "key#{i}", "key#{i}") } }
      wait_for_replication
      count, slot = @client.pipelined { |pi| SLOT_SIZE.times { |i| pi.call('CLUSTER', 'COUNTKEYSINSLOT', i) } }
                           .each_with_index.max_by { |c, _| c }
      refute_equal(0, count)
      keys = @client.call('CLUSTER', 'GETKEYSINSLOT', slot, count)
      refute_empty(keys)
      src, dest = @controller.select_resharding_target(slot)
      @controller.start_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
      yield(keys)
      @controller.finish_resharding(slot: slot, src_node_key: src, dest_node_key: dest)
    end

    def client_in_transaction?(client)
      !!client.instance_variable_get(:@transaction)
    end
  end

  class PrimaryOnly < TestingWrapper
    include Mixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        middlewares: [CommandCaptureMiddleware],
        custom: { captured_commands: @capture_buffer },
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class ScaleReadRandom < TestingWrapper
    include Mixin

    def test_the_state_of_cluster_resharding
      keys = nil
      do_resharding_test { |ks| keys = ks }
      keys.each { |key| assert_equal(key, @client.call('GET', key), "Case: GET: #{key}") }
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      keys = nil
      do_resharding_test { |ks| keys = ks }
      values = @client.pipelined { |pipeline| keys.each { |key| pipeline.call('GET', key) } }
      keys.each_with_index { |key, i| assert_equal(key, values[i], "Case: GET: #{key}") }
    end

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :random,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        middlewares: [CommandCaptureMiddleware],
        custom: { captured_commands: @capture_buffer },
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class ScaleReadRandomWithPrimary < TestingWrapper
    include Mixin

    def test_the_state_of_cluster_resharding
      keys = nil
      do_resharding_test { |ks| keys = ks }
      keys.each { |key| assert_equal(key, @client.call('GET', key), "Case: GET: #{key}") }
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      keys = nil
      do_resharding_test { |ks| keys = ks }
      values = @client.pipelined { |pipeline| keys.each { |key| pipeline.call('GET', key) } }
      keys.each_with_index { |key, i| assert_equal(key, values[i], "Case: GET: #{key}") }
    end

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :random_with_primary,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        middlewares: [CommandCaptureMiddleware],
        custom: { captured_commands: @capture_buffer },
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class ScaleReadLatency < TestingWrapper
    include Mixin

    def test_the_state_of_cluster_resharding
      keys = nil
      do_resharding_test { |ks| keys = ks }
      keys.each { |key| assert_equal(key, @client.call('GET', key), "Case: GET: #{key}") }
    end

    def test_the_state_of_cluster_resharding_with_pipelining
      keys = nil
      do_resharding_test { |ks| keys = ks }
      values = @client.pipelined { |pipeline| keys.each { |key| pipeline.call('GET', key) } }
      keys.each_with_index { |key, i| assert_equal(key, values[i], "Case: GET: #{key}") }
    end

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        replica: true,
        replica_affinity: :latency,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        middlewares: [CommandCaptureMiddleware],
        custom: { captured_commands: @capture_buffer },
        **TEST_GENERIC_OPTIONS
      ).new_client
    end
  end

  class Pooled < TestingWrapper
    include Mixin

    private

    def new_test_client
      ::RedisClient.cluster(
        nodes: TEST_NODE_URIS,
        fixed_hostname: TEST_FIXED_HOSTNAME,
        middlewares: [CommandCaptureMiddleware],
        custom: { captured_commands: @capture_buffer },
        **TEST_GENERIC_OPTIONS
      ).new_pool(timeout: TEST_TIMEOUT_SEC, size: 2)
    end
  end
end
