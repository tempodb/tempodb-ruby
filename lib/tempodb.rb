require 'net/https'
require 'json'
require 'uri'

module TempoDB
    API_HOST = "api.tempo-db.com"
    API_PORT = 443
    API_VERSION = "v1"

    TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), 'trusted-certs.crt')
end

class TempoDBClientError < RuntimeError
    attr_accessor :http_response, :error, :user_error
    def initialize(error, http_response=nil, user_error=nil)
        @error = error
        @http_response = http_response
        @user_error = user_error
    end

    def to_s
        return "#{user_error} (#{error})" if user_error
        "#{error}"
    end
end

class Database
    attr_accessor :key, :secret

    def initialize(key, secret)
        @key = key
        @secret = secret
    end
end

class Series
    attr_accessor :id, :key, :attributes, :tags

    def initialize(id, key, attributes={}, tags=[])
        @id = id
        @key = key
        @attributes = attributes
        @tags = tags
    end
end

class DataPoint
    attr_accessor :ts, :value

    def initialize(ts, value)
        @ts = ts
        @value = value
    end
end

class TempoDBClient
    def initialize(key, secret, host=TempoDB::API_HOST, port=TempoDB::API_PORT, secure=true)
        @key = key
        @secret = secret
        @host = host
        @port = port
        @secure = secure
    end

    def get_series()
        json = do_get('/series/')
        json.map {|series| Series.new(series["id"], series["key"])}
    end

    private

    def do_http(uri, request) # :nodoc:
        http = Net::HTTP.new(uri.host, uri.port)

        http.use_ssl = @secure
        enable_cert_checking(http)
        http.ca_file = TempoDB::TRUSTED_CERT_FILE

        request.basic_auth @key, @secret

        begin
            response = http.request(request)
        rescue OpenSSL::SSL::SSLError => e
            raise TempoDBClientError.new("SSL error connecting to TempoDB.  " +
                                   "There may be a problem with the set of certificates in \"#{TempoDB::TRUSTED_CERT_FILE}\".  " + e)
        end

        parse_response(response)
    end

    def do_get(url, params=nil, headers=nil)  # :nodoc:
        uri = build_uri(url, params)
        do_http(uri, Net::HTTP::Get.new(uri.request_uri, headers))
    end

    def do_http_with_body(uri, request, body)
        if body != nil
            if body.is_a?(Hash)
                form_data = {}
                body.each {|k,v| form_data[k.to_s] = v if !v.nil?}
                request.set_form_data(form_data)
            elsif body.respond_to?(:read)
                if body.respond_to?(:length)
                    request["Content-Length"] = body.length.to_s
                elsif body.respond_to?(:stat) && body.stat.respond_to?(:size)
                    request["Content-Length"] = body.stat.size.to_s
                else
                    raise ArgumentError, "Don't know how to handle 'body' (responds to 'read' but not to 'length' or 'stat.size')."
                end
                request.body_stream = body
            else
                s = body.to_s
                request["Content-Length"] = s.length
                request.body = s
            end
        end
        do_http(uri, request)
    end

    def do_post(url, headers=nil, body=nil)  # :nodoc:
        uri = build_uri(url)
        do_http_with_body(uri, Net::HTTP::Post.new(uri.request_uri, headers), body)
    end

    def do_put(url, headers=nil, body=nil)  # :nodoc:
        uri = build_uri(url)
        do_http_with_body(uri, Net::HTTP::Put.new(uri.request_uri, headers), body)
    end

    def build_uri(url, params=nil)
        versioned_url = "/#{TempoDB::API_VERSION}#{url}"
        protocol = @secure ? "https" : "http"
        target = URI::Generic.new(protocol, nil, @host, @port, nil, versioned_url, nil, nil, nil)

        if params
            target.query = params.collect {|k,v| URI.escape(k) + "=" + URI.escape(v) }.join("&")
        end
        URI.parse(target.to_s)
    end

    def parse_response(response)
        if response.kind_of?(Net::HTTPSuccess)
            begin
                return JSON.parse(response.body)
            rescue JSON::ParserError
                return response.body
            end
        else
            raise TempoDBClientError.new("Invalid response #{response}\n#{response.body}", response)
        end
    end

    def enable_cert_checking(http)
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
    end
end
