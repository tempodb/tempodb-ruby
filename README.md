# TempoDB Ruby API Client

The TempoDB Ruby API Client makes calls to the [TempoDB API](http://tempo-db.com/api/).  The module is available as a gem.

``
gem build tempodb.gemspec
gem install tempodb-1.0.0.gem
``

[![Build Status](https://travis-ci.org/tempodb/tempodb-ruby.png?branch=master)](https://travis-ci.org/tempodb/tempodb-ruby)

# Quickstart

```ruby
require 'tempodb'

client = TempoDB::Client.new('database_id', 'api_key', 'api_secret')
client.create_series("temp-1')
client.list_series
```

# Usage

For more example usage, please see the [TempoDB API documentation](http://tempo-db.com/docs/api/), which includes Ruby client examples for all endpoints.

There is also a [generated RDoc API documentation](http://tempo-db.com/rdoc) which you can use as a reference.
