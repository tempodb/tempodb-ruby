gem 'rdoc', '>= 2.4.2'
require 'rdoc/task'
require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec)
task :default => :spec

desc 'Generate API documentation'
RDoc::Task.new do |rd|
  rd.options << '--inline-source'
  rd.options << '--line-numbers'
end
