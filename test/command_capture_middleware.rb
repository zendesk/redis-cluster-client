# frozen_string_literal: true

module CommandCaptureMiddleware
  CapturedCommand = Struct.new(:server_url, :command, :pipelined, keyword_init: true) do
    def inspect
      "#<#{self.class.name} [on #{server_url}] #{command.join(' ')} >"
    end
  end

  def call(command, redis_config)
    redis_config.custom[:captured_commands] << CapturedCommand.new(
      server_url: CommandCaptureMiddleware.normalize_captured_url(redis_config.server_url),
      command: command,
      pipelined: false
    )
    super
  end

  def call_pipelined(commands, redis_config)
    commands.map do |command|
      redis_config.custom[:captured_commands] << CapturedCommand.new(
        server_url: CommandCaptureMiddleware.normalize_captured_url(redis_config.server_url),
        command: command,
        pipelined: true
      )
    end
    super
  end

  def self.normalize_captured_url(url)
    URI.parse(url).tap do |u|
      u.path = ''
    end.to_s
  end
end
