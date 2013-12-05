# -*- encoding: utf-8 -*-
require File.expand_path('../lib/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Yurii Rashkovskii"]
  gem.email         = ["yrashk@spawngrid.com"]
  gem.description   = %q{Redis backend for DelayedJob}
  gem.summary       = %q{Redis backend for DelayedJob}
  gem.homepage      = "https://github.com/spawngrid/delayed_job_redis"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.name          = "delayed_job_redis"
  gem.require_paths = ["lib"]
  gem.version       = DelayedJobRedis::VERSION

  gem.add_runtime_dependency   'uuidtools'
  gem.add_runtime_dependency   'redis', '3.0.2'
  gem.add_runtime_dependency   'delayed_job',  '~> 4.0'

  gem.add_development_dependency 'rake', '10.0.3'
  gem.add_development_dependency 'rspec', '2.11.0'
  gem.add_development_dependency 'sqlite3'
  gem.add_development_dependency 'rails', '3.2.12'
  gem.add_development_dependency 'debugger'

end
