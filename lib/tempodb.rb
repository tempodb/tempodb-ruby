require 'rubygems'
require 'httpclient'
require 'net/https'
require 'json'
require 'time'
require 'uri'

require 'tempodb/session'
require 'tempodb/client'
require 'tempodb/cursor'
require 'tempodb/version'
require 'tempodb/series'
require 'tempodb/single_value'
require 'tempodb/data_point'
require 'tempodb/data_point_found'
require 'tempodb/data_set'
require 'tempodb/delete_summary'
require 'tempodb/multipoint'
require 'tempodb/multipoint_segment'
require 'tempodb/multi_write'
require 'tempodb/series_summary'
require 'tempodb/summary'

module TempoDB
  API_HOST = "api.tempo-db.com"
  API_PORT = 443
  API_VERSION = "v1"

  TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), "trusted-certs.crt")
end

