require 'rubygems'
require 'httpclient'
require 'net/https'
require 'json'
require 'time'
require 'uri'

require 'tempodb/version'
require 'tempodb/series'
require 'tempodb/client'

module TempoDB
  API_HOST = "api.tempo-db.com"
  API_PORT = 443
  API_VERSION = "v1"

  TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), "trusted-certs.crt")

  class Database
    attr_accessor :key, :secret

    def initialize(key, secret)
      @key = key
      @secret = secret
    end

    def to_json(*a)
      { "id" => key, "password" => secret }.to_json(*a)
    end

    def self.from_json(m)
      new(m["id"], m["password"])
    end
  end

  class DataPoint
    attr_accessor :ts, :value

    def initialize(ts, value)
      @ts = ts
      @value = value
    end

    def to_json(*a)
      {"t" => ts.iso8601(3), "v" => value}
    end

    def self.from_json(m)
      new(Time.parse(m["t"]), m["v"])
    end
  end

  class DataSet
    attr_accessor :series, :start, :stop, :data, :summary

    def initialize(series, start, stop, data=[], summary=nil)
      @series = series
      @start = start
      @stop = stop
      @data = data
      @summary = summary
    end

    def self.from_json(m)
      series = Series.from_json(m["series"])
      start = Time.parse(m["start"])
      stop = Time.parse(m["end"])
      data = m["data"].map {|dp| DataPoint.from_json(dp)}
      summary = Summary.from_json(m["summary"])
      new(series, start, stop, data, summary)
    end
  end

  class Summary

    def initialize()
    end

    def self.from_json(m)
      summary = Summary.new()
      m.each do |k, v|
        summary.instance_variable_set("@#{k}", v)  ## create and initialize an instance variable for this key/value pair
        summary.class.send(:define_method, k, proc{self.instance_variable_get("@#{k}")})  ## create the getter that returns the instance variable
        summary.class.send(:define_method, "#{k}=", proc{|v| self.instance_variable_set("@#{k}", v)})  ## create the setter that sets the instance variable
      end
      summary
    end
  end
end
