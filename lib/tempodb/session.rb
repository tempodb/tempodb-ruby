require 'httpclient'
require 'net/https'
require 'uri'

module TempoDB
  class Session
    def initialize(key, secret, host, port, secure)
      @key = key
      @secret = secret
      @host = host
      @port = port
      @secure = secure
      @http_client = HTTPClient.new
      if secure
        @http_client.ssl_config.clear_cert_store
        @http_client.ssl_config.set_trust_ca(TempoDB::TRUSTED_CERT_FILE)
      end
    end

    def cursored_get(uri, headers=nil)  # :nodoc:
      do_http(uri, Net::HTTP::Get.new(uri.request_uri, headers))
    end

    def build_uri(url_parts, params=nil)
      versioned_url_parts = [TempoDB::API_VERSION] + url_parts
      url = versioned_url_parts.map do |part|
        URI.escape(part, Regexp.new("[^#{URI::REGEXP::PATTERN::UNRESERVED}]", false))
      end.join('/')
      target = construct_uri(url)
      if params
        target.query = urlencode(params)
      end
      URI.parse(target.to_s)
    end

    def do_get(url, params = nil, headers = nil)  # :nodoc:
      uri = build_uri(url, params)
      parse_response(do_http(uri, Net::HTTP::Get.new(uri.request_uri, headers)))
    end

    def do_delete(url, params = nil, headers = nil)
      uri = build_uri(url, params)
      parse_response(do_http(uri, Net::HTTP::Delete.new(uri.request_uri, headers)))
    end

    def do_post(url_parts, headers = nil, body = nil)  # :nodoc:
      uri = build_uri(url_parts)
      do_http_with_body(uri, Net::HTTP::Post.new(uri.request_uri, headers), body)
    end

    def do_put(url_parts, headers = nil, body = nil)  # :nodoc:
      uri = build_uri(url_parts)
      do_http_with_body(uri, Net::HTTP::Put.new(uri.request_uri, headers), body)
    end

    def construct_uri(url)
      protocol = @secure ? 'https' : 'http'
      URI::Generic.new(protocol, nil, @host, @port, nil, "/#{url}/", nil, nil, nil)
    end

    private

    def do_http(uri, request) # :nodoc:
      if @http_client.nil?
        @http_client = HTTPClient.new
        @http_client.transparent_gzip_decompression = true
        if @secure
          @http_client.ssl_config.clear_cert_store
          @http_client.ssl_config.set_trust_ca(TempoDB::TRUSTED_CERT_FILE)
        end
      end

      request.basic_auth @key, @secret
      request['User-Agent'] = "tempodb-ruby/#{TempoDB::VERSION}"
      request['Accept-Encoding'] = "gzip"

      method = request.method.downcase.intern
      http_client_attrs = {
        :header => Hash[request.to_hash.map {|k,v| [k, v.first] if v.is_a?(Array) && v.size == 1}],
        :body => request.body
      }

      begin
        response = @http_client.request(method, uri, http_client_attrs)
      rescue OpenSSL::SSL::SSLError => e
        raise TempoDBClientError.new("SSL error connecting to TempoDB.  " +
                                     "There may be a problem with the set of certificates in \"#{TempoDB::TRUSTED_CERT_FILE}\".  " + e)
      end

      response
    end

    def do_http_with_body(uri, request, body)
      if body != nil
        if body.is_a?(String)
          s = body.to_s
          request["Content-Length"] = s.length
          request.body = s
        else
          s = JSON.dump(body)
          request["Content-Length"] = s.length
          request["Content-Type"] = "application/json"
          request.body = s
        end
      end
      parse_response(do_http(uri, request))
    end

    def urlencode(params)
      p = []
      params.each do |key, value|
        if value.is_a? Array
          value.each { |v| p.push(URI.escape(key.to_s) + "=" + URI.escape(v)) }
        elsif value.is_a? Hash
          value.each { |k, v| p.push("#{URI.escape(key.to_s)}[#{URI.escape(k.to_s)}]=#{URI.escape(v)}") }
        else
          p.push(URI.escape(key.to_s) + "=" + URI.escape(value.to_s))
        end
      end
      p.join("&")
    end

    def parse_response(response)
      if response.ok?
        body = response.body

        if body.empty?
          return {}
        else
          return JSON.parse(body)
        end
      elsif response.status == 207
        raise TempoDBMultiStatusError.new(response.status, JSON.parse(response.body))
      else
        raise TempoDBClientError.new("Error: #{response.status_code} #{response.reason}\n#{response.body}")
      end
    end
  end
end
