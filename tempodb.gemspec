# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "tempodb/version"

Gem::Specification.new do |s|
  s.name        = "tempodb"
  s.version     = TempoDB::VERSION
  s.licenses    = ['MIT']
  s.authors     = ["TempoDB, Inc."]
  s.email       = ["software@tempo-db.com"]
  s.homepage    = "http://tempo-db.com"
  s.summary     = %q{A client for TempoDB}
  s.description = %q{Ruby client for TempoDB's HTTP API}

  s.files         = `git ls-files`.split("\n")
  s.require_paths = ["lib"]

  s.add_runtime_dependency "httpclient", "2.3.4"
  s.add_runtime_dependency "httpclient_link_header", "0.0.1"
  s.add_development_dependency "rspec", "~> 2.12"
  s.add_development_dependency "webmock", "~> 1.8"
end
