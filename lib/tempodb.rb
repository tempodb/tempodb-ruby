require 'rubygems'
require 'httpclient'
require 'net/https'
require 'json'
require 'time'
require 'uri'

require 'tempodb/client'
require 'tempodb/version'
require 'tempodb/series'
require 'tempodb/data_point'
require 'tempodb/data_set'
require 'tempodb/delete_summary'
require 'tempodb/summary'

module TempoDB
  API_HOST = "api.tempo-db.com"
  API_PORT = 443
  API_VERSION = "v1"

  TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), "trusted-certs.crt")
end

