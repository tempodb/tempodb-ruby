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

      json = do_post(["series"], nil, params)
      Series.from_json(json)
    end

    def list_series(params={})
      TempoDB::Cursor.new(build_uri(["series"], attribute_params(params)), self, SeriesCursor, Series)
    end

    def delete_series(params)
      json = do_delete(["series"], attribute_params(params))
      DeleteSummary.from_json(json)
    end

    def update_series(series)
      json = do_put(["series", "id", series.id], nil, series.to_json)
      Series.from_json(json)
    end

    def read_data(series_key, start, stop, options={})
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

      url = ["series", "key", series_key, "segment"]
      TempoDB::Cursor.new(build_uri(url, params), self, DataPointCursor, DataSet)
    end

    def read_multi(start, stop, options = {})
      params = rollup_params(attribute_params(options))
      params[:start] = start.iso8601(3)
      params[:stop] = stop.iso8601(3)
      url = ["multi"]
      TempoDB::Cursor.new(build_uri(url, params), self, MultiPointCursor, MultiPointSegment)
    end

    def delete_key(series_key, start, stop, options={})
      series_type = "key"
      series_val = series_key
      _delete(series_type, series_val, start, stop, options)
    end

    def write_key(series_key, data)
      series_type= 'key'
      series_val = series_key
      write(series_type, series_val, data)
    end

    def write_bulk(ts, data)
      json = JSON.generate({
        :t => ts.iso8601(3),
        :data => data
      })
      url = ["data"]
      do_post(url, nil, json)
    end

    def write_multi(data)
      url = ["multi"]
      do_multi(url, data)
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
      url = ["increment"]
      do_post(url, nil, json)
    end

    def increment_multi(data)
      url = ["multi","increment"]
      do_multi(url, data)
    end

    def cursored_get(uri, headers=nil)  # :nodoc:
      do_http(uri, Net::HTTP::Get.new(uri.request_uri, headers))
    end

    def build_uri(url_parts, params=nil)
      versioned_url_parts = [TempoDB::API_VERSION] + url_parts
      url = versioned_url_parts.map do |part|
        URI.escape(part, Regexp.new("[^#{URI::REGEXP::PATTERN::UNRESERVED}]", false))
      end.join("/")
      target = construct_uri(url)
      if params
        target.query = urlencode(params)
      end
      URI.parse(target.to_s)
    end

    def construct_uri(url)
      protocol = @secure ? "https" : "http"
      URI::Generic.new(protocol, nil, @host, @port, nil, "/#{url}/", nil, nil, nil)
    end

    private

    # Takes an input params hash, applies the mapping hash to transform key names and pass
    # through all other unrecognized hash key entries
    def map_params(params, mapping)
      cloned = params.clone
      p = {}
      mapping.each do |from, to|
        value = cloned[from]
        p[to] = value if value
        cloned.delete(from)
      end
      p.merge(cloned)
    end

    def attribute_params(params)
      map_params(params,
                 :ids => 'id',
                 :keys => 'key',
                 :tags => 'tag',
                 :attributes => 'attr')
    end

    def rollup_params(params)
      map_params(params,
                 :rollup_function => 'rollup.fold',
                 :rollup_period => 'rollup.period',
                 :interpolation_function => 'interpolation.function',
                 :interpolation_period => 'interpolation.period')
    end

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

      url = ["series", series_type, series_val, "data"]
      json = do_get(url, params)
      DataSet.from_json(json)
    end

    def _delete(series_type, series_val, start, stop, options={})
      defaults = {}
      options = defaults.merge(options)

      params = {}
      params[:start] = start.iso8601(3)
      params[:end] = stop.iso8601(3)

      url = ["series", series_type, series_val, "data"]
      do_delete(url, params)
    end

    def write(series_type, series_val, data)
      url = ["series", series_type, series_val, "data"]
      body = data.collect {|dp| dp.to_json()}
      do_post(url, nil, body)
    end

    def increment(series_type, series_val, data)
      url = ["series", series_type, series_val, "increment"]
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

      response
    end

    def do_http_deprecated(uri, request) # :nodoc:
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
      do_http_deprecated(uri, Net::HTTP::Get.new(uri.request_uri, headers))
    end

    def do_delete(url, params=nil, headers=nil)
      uri = build_uri(url, params)
      do_http_deprecated(uri, Net::HTTP::Delete.new(uri.request_uri, headers))
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
      do_http_deprecated(uri, request)
    end

    def do_post(url_parts, headers=nil, body=nil)  # :nodoc:
      uri = build_uri(url_parts)
      do_http_with_body(uri, Net::HTTP::Post.new(uri.request_uri, headers), body)
    end

    def do_put(url_parts, headers=nil, body=nil)  # :nodoc:
      uri = build_uri(url_parts)
      do_http_with_body(uri, Net::HTTP::Put.new(uri.request_uri, headers), body)
    end

    def do_multi(url_parts, datapoints)
      converted = datapoints.map do |dp|
        dp_copy = dp.clone
        if dp_copy.has_key?(:t)
          ts = dp_copy[:t].iso8601(3)
          dp_copy[:t] = ts
        end
        dp_copy
      end
      do_post(url_parts, nil, JSON.generate(converted))
    end

    def urlencode(params)
      p = []
      params.each do |key, value|
        if value.is_a? Array
          value.each {|v| p.push(URI.escape(key.to_s) + "=" + URI.escape(v))}
        elsif value.is_a? Hash
          value.each {|k, v| p.push("#{URI.escape(key.to_s)}[#{URI.escape(k.to_s)}]=#{URI.escape(v)}")}
        else
          p.push(URI.escape(key.to_s) + "=" + URI.escape(value.to_s))
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
      elsif response.status == 207
        raise TempoDBMultiStatusError.new(response.status, JSON.parse(response.body))
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

  class TempoDBMultiStatusError < RuntimeError
    attr_accessor :http_response, :multi_status_response
    def initialize(http_response, multi_status_response)
      @http_response = http_response
      @multi_status_response = multi_status_response
    end
  end
end

