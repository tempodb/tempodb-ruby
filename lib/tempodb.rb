require 'rubygems'

require 'tempodb/client'

module TempoDB
  API_HOST = "api.tempo-db.com"
  API_PORT = 443
  API_VERSION = "v1"

  TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), "trusted-certs.crt")
end

