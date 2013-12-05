require 'uuidtools'
module Delayed
  module Backend
    module RedisStore
      class Job
        include Delayed::Backend::Base
        attr_accessor :priority, :run_at, :queue,
          :failed_at, :locked_at, :locked_by

        attr_accessor :handler
        attr_writer :id

        attr_accessor :last_error, :attempts

        def self.set_name
          "set_#{_redis_prefix.call}"
        end

        def self.add_to_set(_key)
          redis.send(:sadd, set_name, _key)
        end

        def self.get_random_key
          redis.send(:srandmember, set_name)
        end

        def self.remove_from_set(_key)
          redis.send(:srem, set_name, _key)
        end

        def self.forget_keys
          @know_keys = false
          redis.send(:del, set_name)
        end

        def self.redis_key(_id)
          "#{_redis_prefix.call}_#{_id}"
        end

        def self.redis
          Delayed::Worker.send(:redis)
        end

        def self.know_keys!
          @know_keys = true
        end

        def self.know_keys?
          !! @know_keys
        end

        def self.all_keys
          unless know_keys?
            learn_keys
          end
          redis.send(:smembers, set_name) || []
        end

        def self._redis_prefix
          lambda { Delayed::Worker.redis_prefix }
        end

        # slow
        def self.learn_keys
          forget_keys
          _keys = search_keys
          if _keys && !_keys.empty?
            add_to_set(_keys)
          end
          know_keys!
        end

        # slow
        def self.search_keys
          _pattern = "#{_redis_prefix.call}_*"
          redis.send(:keys, _pattern) || []
        end

        def self.count
          all_keys.length
        end

        def self.delete_all
          _keys = all_keys
          if _keys && !_keys.empty?
            redis.send(:del, _keys)
          end
          forget_keys
        end

        def self.ready_to_run(worker_name, max_run_time)
          time_now = db_time_now
          keys = all_keys
          keys.select do |key|
            run_at, locked_at, locked_by, failed_at = redis.hmget key, "run_at", "locked_at", "locked_by", "failed_at"
            run_at = Time.at run_at.to_i
            locked_at = Time.at locked_at.to_i
            failed_at = Time.at failed_at.to_i
            (run_at <= time_now and (locked_at.to_i == 0 or locked_at < time_now - max_run_time) or locked_by == worker_name) and failed_at.to_i == 0
          end
        end

        def self.before_fork
          redis.client.disconnect
        end

        def self.after_fork
          redis.client.connect
        end

        # When a worker is exiting, make sure we don't have any locked jobs.
        def self.clear_locks!(worker_name)
          keys = all_keys
          keys = keys.select do |key|
            locked_by = redis.hget key, "locked_by"
            locked_by == worker_name
          end
          keys.each do |k|
            redis.hdel k, "locked_by"
            redis.hdel k, "locked_at"
          end
        end

        # Find a few candidate jobs to run (in case some immediately get locked by others).
        def self.find_available(worker_name, limit = 5, max_run_time = Worker.max_run_time)
          keys = ready_to_run(worker_name, max_run_time)
          if Worker.min_priority
            keys = keys.select do |key|
              priority = redis.hget key, "priority"
              priority = priority.to_i
              priority >= Worker.min_priority
            end
          end

          if Worker.max_priority
            keys = keys.select do |key|
              priority = redis.hget key, "priority"
              priority = priority.to_i
              priority <= Worker.max_priority
            end
          end

          if Worker.queues.any?
            keys = keys.select do |key|
              queue = redis.hget key, "queue"
              Worker.queues.include?(queue)
            end
          end

          keys = keys.sort_by do |key|
            priority, run_at = redis.hmget key, "priority", "run_at"
            priority = priority.to_i
            run_at = run_at.to_i
            [priority, run_at]
          end

          keys[0..limit-1].map {|k| find(k) }
        end

        def save
          set_default_run_at
          attrs = [:id, :priority, :run_at, :queue, :last_error,
                  :failed_at, :locked_at, :locked_by, :attempts].select  {|c| v = self.send(c); !v.nil? }
          args = attrs.map do |k|
            v = self.send(k)
            v = v.to_i if v.is_a?(Time)
            [k.to_s, v]
          end.flatten
          args += ["payload_object", handler]
          _key = self.class.redis_key(id)
          Delayed::Worker.redis.hmset _key, *args
          self.class.add_to_set(_key)
          self
        end

        def save! ; save ; end

        def destroy
          _key = self.class.redis_key(id)
          self.class.remove_from_set(_key)
          Delayed::Worker.redis.del _key
        end

        def id
          unless @id
            @id = UUIDTools::UUID.random_create.to_s
          end
          @id
        end

        def self.create(options)
          new(options).save
        end

        def self.create!(options)
          create(options)
        end

        def reload
          reset
          _key = self.class.redis_key(id)
          _priority, _run_at, _queue, _payload_object, _failed_at, _locked_at, _locked_by, _attempts, _last_error =
            Delayed::Worker.redis.hmget _key, "priority", "run_at",
            "queue", "payload_object", "failed_at", "locked_at", "locked_by", "attempts", "last_error"
          self.priority = _priority.to_i
          self.run_at = _run_at.nil? ? nil : Time.at(_run_at.to_i)
          self.queue = _queue
          self.handler = _payload_object||YAML.dump(nil)
          self.failed_at = _failed_at.nil? ? nil : Time.at(_failed_at.to_i)
          self.locked_at = _locked_at.nil? ? nil : Time.at(_locked_at.to_i)
          self.locked_by = _locked_by
          self.attempts = _attempts.to_i
          self.last_error = _last_error
          self
        end

        def initialize(options)
          @id = nil
          @priority = 0
          @run_at = nil
          @queue = nil
          @failed_at = nil
          @locked_at = nil
          @attempts = 0
          build_attributes(options)
        end

        def build_attributes(_options)
          _options.each {|k,v| send("#{k}=", v) }
        end

        def update_attributes(options)
          build_attributes(options)
          save
        end

        def self.first
          key = get_random_key
          if key && !key.empty?
            find(key)
          else
            nil
          end
        end

        # FIXME UGH ...gonna have to add a "where" method soon
        def self.find(key)
          _, _id = key.split("#{Delayed::Worker.redis_prefix}_")
          _priority, _run_at, _queue, _payload_object, _failed_at, _locked_at, _locked_by, _attempts, _last_error =
            redis.hmget "#{Delayed::Worker.redis_prefix}_#{_id}", "priority", "run_at",
            "queue", "payload_object", "failed_at", "locked_at", "locked_by", "attempts", "last_error"
          new(:id => _id,
              :priority => _priority.to_i,
              :run_at => _run_at.nil? ? nil : Time.at(_run_at.to_i),
              :queue => _queue,
              :handler => _payload_object||YAML.dump(nil),
              :failed_at => _failed_at.nil? ? nil : Time.at(_failed_at.to_i),
              :locked_at => _locked_at.nil? ? nil : Time.at(_locked_at.to_i),
              :locked_by => _locked_by,
              :last_error => _last_error,
              :attempts => _attempts.to_i)
        end

        # Lock this job for this worker.
        # Returns true if we have the lock, false otherwise.
        def lock_exclusively!(max_run_time, worker)
          now = self.class.db_time_now
          keys = self.class.all_keys
          if locked_by != worker
            # We don't own this job so we will update the locked_by name and the locked_at

            keys = keys.select do |key|
              _id, locked_at, run_at = Delayed::Worker.redis.hmget key, "id", "locked_at", "run_at"
              run_at = Time.at(run_at.to_i)
              locked_at = Time.at(locked_at.to_i)
              latest_locked_at = (now - max_run_time.to_i)
              _id == id and (locked_at.to_i == 0 or locked_at < latest_locked_at) and (run_at <= now)
            end

            if !keys.empty?
              Delayed::Worker.redis.watch(*keys)
              Delayed::Worker.redis.multi do
                keys.each {|key| Delayed::Worker.redis.hmset key, "locked_at", now, "locked_by", worker}
              end
            end
          else
            # We already own this job, this may happen if the job queue crashes.
            # Simply resume and update the locked_at

            keys = keys.select do |key|
              _id, locked_by = Delayed::Worker.redis.hmget key, "id", "locked_by"
              _id == id and locked_by == worker
            end

            if !keys.empty?
              Delayed::Worker.redis.watch(*keys)
              Delayed::Worker.redis.multi do
                keys.each {|key| Delayed::Worker.redis.hset key, "locked_at", now }
              end
            end
          end

          affected_rows = keys.length
          if affected_rows == 1
            self.locked_at = now
            self.locked_by = worker
            save
            return true
          else
            return false
          end
        end

        def self.db_time_now
          Time.now
        end

        def ==(x)
          self.id == x.id
        end

      end
    end
  end
end
