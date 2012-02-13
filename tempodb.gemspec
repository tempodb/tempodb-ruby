# -*- encoding: utf-8 -*-
$:.push File.expand_path("../lib", __FILE__)
require "tempodb/version"

Gem::Specification.new do |s|
  s.name        = "tempodb"
  s.version     = TempoDB::VERSION
  s.authors     = ["TempoDB, Inc."]
  s.email       = ["software@tempo-db.com"]
  s.homepage    = "http://tempo-db.com"
  s.summary     = %q{A client for TempoDB}
  s.description = %q{A client for TempoDB}

  s.files         = `git ls-files`.split("\n")
  s.require_paths = ["lib"]

  s.add_runtime_dependency "json"
end
