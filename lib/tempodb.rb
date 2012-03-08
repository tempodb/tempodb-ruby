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
        attr_accessor :id, :key, :name, :attributes, :tags

        def initialize(id, key, name="", attributes={}, tags=[])
            @id = id
            @key = key
            @name = name
            @attributes = attributes
            @tags = tags
        end

        def to_json(*a)
            { "id" => id, "key" => key, "name" => name, "attributes" => attributes, "tags" => tags }.to_json(*a)
        end

        def self.from_json(m)
            new(m["id"], m["key"], m["name"], m["attributes"], m["tags"])
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

    class Client
        def initialize(key, secret, host=TempoDB::API_HOST, port=TempoDB::API_PORT, secure=true)
            @key = key
            @secret = secret
            @host = host
            @port = port
            @secure = secure
        end

        def get_series(options={})
            defaults = {
                :ids => [],
                :keys => [],
                :tags => [],
                :attributes => {}
            }
            options = defaults.merge(options)

            params = {}
            if options[:ids] then params[:id] = options[:ids] end
            if options[:keys] then params[:key] = options[:keys] end
            if options[:tags] then params[:tag] = options[:tags] end
            if options[:attributes] then params[:attr] = options[:attributes] end

            json = do_get("/series/", params)
            json.map {|series| Series.from_json(series)}
        end

        def update_series(series)
            json = do_put("/series/id/#{series.id}/", nil, series.to_json())
            Series.from_json(json)
        end

        def read(start, stop, options={})
            defaults = {
                :interval => "",
                :function => "",
                :ids => [],
                :keys => [],
                :tags => [],
                :attributes => {}
            }
            options = defaults.merge(options)

            params = {}
            params[:start] = start.iso8601(3)
            params[:end] = stop.iso8601(3)
            params[:interval] = options[:interval] if options[:interval]
            params[:function] = options[:function] if options[:function]
            params[:id] = options[:ids] if options[:ids]
            params[:key] = options[:keys] if options[:keys]
            params[:tag] = options[:tags] if options[:tags]
            params[:attr] = options[:attributes] if options[:attributes]

            url = "/data/"
            json = do_get(url, params)

            json.map {|ds| DataSet.from_json(ds)}
        end

        def read_id(series_id, start, stop, options={})
            series_type = "id"
            series_val = series_id
            _read(series_type, series_val, start, stop, options)
        end

        def read_key(series_key, start, stop, options={})
            series_type = "key"
            series_val = series_key
            _read(series_type, series_val, start, stop, options)
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

        def _read(series_type, series_val, start, stop, options={})
            defaults = {
                :interval => "",
                :function => "",
            }
            options = defaults.merge(options)

            params = {}
            params[:start] = start.iso8601(3)
            params[:end] = stop.iso8601(3)
            params[:interval] = options[:interval] if options[:interval]
            params[:function] = options[:function] if options[:function]

            url = "/series/#{series_type}/#{series_val}/data/"
            json = do_get(url, params)
            DataSet.from_json(json)
        end

        def write(series_type, series_val, data)
            url = "/series/#{series_type}/#{series_val}/data/"
            body = data.collect {|dp| dp.to_json()}
            do_post(url, nil, body)
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
                target.query = urlencode(params)
            end
            URI.parse(target.to_s)
        end

        def urlencode(params)
            p = []
            params.each do |key, value|
                if value.is_a? Array
                    value.each {|v| p.push(URI.escape(key.to_s) + "=" + URI.escape(v))}
                elsif value.is_a? Hash
                    value.each {|k, v| p.push("#{URI.escape(key.to_s)}[#{URI.escape(k.to_s)}]=#{URI.escape(v)}")}
                else
                    p.push(URI.escape(key.to_s) + "=" + URI.escape(value))
                end
            end
            p.join("&")
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

end
