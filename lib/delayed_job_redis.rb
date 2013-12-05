require "redis"
require "delayed_job"

module Delayed
  class Worker
    class << self
      attr_reader :redis
      attr_accessor :redis_prefix
      def redis=(val)
        @redis = val
        Delayed::Backend::RedisStore::Job.learn_keys
      end
    end
  end
end

Delayed::Worker.backend = :redis_store
Delayed::Worker.redis_prefix = "delayed_job"
require "delayed/backend/redis_store"
