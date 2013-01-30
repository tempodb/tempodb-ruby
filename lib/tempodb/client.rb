module TempoDB
  class Client
    def initialize(key, secret, host=TempoDB::API_HOST, port=TempoDB::API_PORT, secure=true)
      @key = key
      @secret = secret
      @host = host
      @port = port
      @secure = secure
    end

    def create_series(key=nil)
      params = {}
      if key != nil
        params[:key] = key
      end

      json = do_post("/series/", nil, params)
      Series.from_json(json)
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
      json = do_put("/series/id/#{series.id}/", nil, series.to_json)
      Series.from_json(json)
    end

    def read(start, stop, options={})
      defaults = {
        :interval => "",
        :function => "",
        :tz => "",
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
      params[:tz] = options[:tz] if options[:tz]
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

    def delete_id(series_id, start, stop, options={})
      series_type = "id"
      series_val = series_id
      _delete(series_type, series_val, start, stop, options)
    end

    def delete_key(series_key, start, stop, options={})
      series_type = "key"
      series_val = series_key
      _delete(series_type, series_val, start, stop, options)
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
      json = JSON.generate({
        :t => ts.iso8601(3),
        :data => data
      })
      url = "/data/"
      do_post(url, nil, json)
    end

    def increment_id(series_id, data)
      series_type = 'id'
      series_val = series_id
      increment(series_type, series_val, data)
    end

    def increment_key(series_key, data)
      series_type = 'key'
      series_val = series_key
      increment(series_type, series_val, data)
    end

    def increment_bulk(ts, data)
      json = JSON.generate({
        :t => ts.iso8601(3),
        :data => data
      })
      url = "/increment/"
      do_post(url, nil, json)
    end

    private

    def _read(series_type, series_val, start, stop, options={})
      defaults = {
        :interval => "",
        :function => "",
        :tz => ""
      }
      options = defaults.merge(options)

      params = {}
      params[:start] = start.iso8601(3)
      params[:end] = stop.iso8601(3)
      params[:interval] = options[:interval] if options[:interval]
      params[:function] = options[:function] if options[:function]
      params[:tz] = options[:tz] if options [:tz]

      url = "/series/#{series_type}/#{series_val}/data/"
      json = do_get(url, params)
      DataSet.from_json(json)
    end

    def _delete(series_type, series_val, start, stop, options={})
      defaults = {}
      options = defaults.merge(options)

      params = {}
      params[:start] = start.iso8601(3)
      params[:end] = stop.iso8601(3)

      url = "/series/#{series_type}/#{series_val}/data/"
      do_delete(url, params)
    end

    def write(series_type, series_val, data)
      url = "/series/#{series_type}/#{series_val}/data/"
      body = data.collect {|dp| dp.to_json()}
      do_post(url, nil, body)
    end

    def increment(series_type, series_val, data)
      url = "/series/#{series_type}/#{series_val}/increment/"
      body = data.collect {|dp| dp.to_json()}
      do_post(url, nil, body)
    end

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

      parse_response(response)
    end

    def do_get(url, params=nil, headers=nil)  # :nodoc:
      uri = build_uri(url, params)
      do_http(uri, Net::HTTP::Get.new(uri.request_uri, headers))
    end

    def do_delete(url, params=nil, headers=nil)
      uri = build_uri(url, params)
      do_http(uri, Net::HTTP::Delete.new(uri.request_uri, headers))
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
      if response.ok?
        body = response.body

        begin
          if body == ""
            return {}
          else
            return JSON.parse(body)
          end
        rescue JSON::ParserError
          return body
        end
      else
        raise TempoDBClientError.new("Error: #{response.status_code} #{response.reason}\n#{response.body}")
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

