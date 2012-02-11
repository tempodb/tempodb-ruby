require 'rubygems'
require 'net/https'
require 'json'
require 'time'
require 'uri'

module TempoDB
    API_HOST = "api.tempo-db.com"
    API_PORT = 443
    API_VERSION = "v1"

    TRUSTED_CERT_FILE = File.join(File.dirname(__FILE__), "trusted-certs.crt")
end

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

class Series
    attr_accessor :id, :key, :attributes, :tags

    def initialize(id, key, attributes={}, tags=[])
        @id = id
        @key = key
        @attributes = attributes
        @tags = tags
    end

    def to_json(*a)
        { "id" => id, "key" => key, "attributes" => attributes, "tags" => tags }.to_json(*a)
    end

    def self.from_json(m)
        new(m["id"], m["key"], m["attributes"], m["tags"])
    end
end

class DataPoint
    attr_accessor :ts, :value

    def initialize(ts, value)
        @ts = ts
        @value = value
    end

    def to_json(*a)
        "{\"t\":\"#{ts.iso8601(3)}\",\"v\":#{value}}"
    end

    def self.from_json(m)
        new(Time.parse(m["t"]), m["v"])
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
        json = do_get("/series/")
        json.map {|series| Series.from_json(series)}
    end

    def read_id(series_id, start, stop, interval="", function="")
        series_type = "id"
        series_val = series_id
        read(series_type, series_val, start, stop, interval, function)
    end

    def read_key(series_key, start, stop, interval="", function="")
        series_type = "key"
        series_val = series_key
        read(series_type, series_val, start, stop, interval, function)
    end

    def write_id(series_id, data)
        series_type = 'id'
        series_val = series_id
        write(series_type, series_val, data)
    end

    def write_key(series_key, data)
        series_type = 'key'
        series_val = series_key
        write(series_type, series_val, data)
    end

    def write_bulk(ts, data)
        items = data.map do |item|
            id = item["id"] ? "\"id\":\"#{item["id"]}\"," : ""
            key = item["key"] ? "\"key\":\"#{item["key"]}\"," : ""
            value = item["v"] ? "\"v\":#{item["v"]}" : ""
            "{" + id + key + value + "}"
        end
        d = "[" + items.join(",") + "]"
        json = "{\"t\":\"#{ts.iso8601(3)}\",\"data\":#{d}}"

        url = "/data/"
        do_post(url, nil, json)
    end

    private

    def read(series_type, series_val, start, stop, interval="", function="")
        params = {
            "start" => start.iso8601,
            "end" => stop.iso8601
        }

        # add rollup interval and function if supplied
        params["interval"] = interval if interval
        params["function"] = function if function

        url = "/series/#{series_type}/#{series_val}/data/"
        json = do_get(url, params)

        json.map {|dp| DataPoint.from_json(dp)}
    end

    def write(series_type, series_val, data)
        url = "/series/#{series_type}/#{series_val}/data/"
        do_post(url, nil, data)
    end

    def do_http(uri, request) # :nodoc:
        http = Net::HTTP.new(uri.host, uri.port)

        http.use_ssl = @secure
        http.verify_mode = OpenSSL::SSL::VERIFY_PEER
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
                if response.body == ""
                    return {}
                else
                    return JSON.parse(response.body)
                end
            rescue JSON::ParserError
                return response.body
            end
        else
            raise TempoDBClientError.new("Invalid response #{response}\n#{response.body}", response)
        end
    end
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
