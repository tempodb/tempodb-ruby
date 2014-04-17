require 'time'
require 'json'

require 'tempodb/cursor'
require 'tempodb/data_point'
require 'tempodb/data_point_found'
require 'tempodb/data_set'
require 'tempodb/delete_summary'
require 'tempodb/multipoint'
require 'tempodb/multipoint_segment'
require 'tempodb/multi_write'
require 'tempodb/series'
require 'tempodb/series_summary'
require 'tempodb/session'
require 'tempodb/single_value'
require 'tempodb/version'

module TempoDB
  class Client
    attr_reader :session

    def initialize(key, secret, host = TempoDB::API_HOST, port = TempoDB::API_PORT, secure = true)
      @session = Session.new(key, secret, host, port, secure)
    end

    def create_series(series_key = nil)
      params = {}
      params['key'] = series_key if series_key
      json = session.do_post(['series'], nil, params)
      Series.from_json(json)
    end

    def delete_series(params)
      json = session.do_delete(['series'], attribute_params(params))
      DeleteSummary.from_json(json)
    end

    def get_series(series_key)
      json = session.do_get(['series', 'key', series_key])
      Series.from_json(json)
    end

    def list_series(options = {})
      Cursor.new(session.build_uri(['series'], attribute_params(options)), session, ArrayCursor, Series)
    end

    def update_series(series)
      json = session.do_put(['series', 'id', series.id], nil, series.to_json)
      Series.from_json(json)
    end

    def read_data(series_key, start, stop, options = {})
      params = {}
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      params['interval'] = options[:interval] if options[:interval]
      params['function'] = options[:function] if options[:function]
      params['tz'] = options[:tz] if options [:tz]

      url = ['series', 'key', series_key, 'segment']
      Cursor.new(session.build_uri(url, params), session, DataCursor, DataSet, 'start', 'end', 'rollup', 'series')
    end

    def get_summary(series_key, start, stop, options = {})
      params = {}
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      params['tz'] = options[:tz] if options[:tz]
      json = session.do_get(["series", "key", series_key, "summary"], params)
      SeriesSummary.from_json(json)
    end

    def read_multi_rollups(series_key, start, stop, options = {})
      params = rollup_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'data', 'rollups', 'segment']
      Cursor.new(session.build_uri(url, params), session, DataCursor, MultiPointSegment, 'series')
    end

    def find_data(series_key, start, stop, options = {})
      params = find_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'find']
      Cursor.new(session.build_uri(url, params), session, DataCursor, DataPointFound, 'find', 'predicate')
    end

    def aggregate_data(aggregation, start, stop, options = {})
      url = ['segment']
      params = rollup_params((attribute_params(options)))
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      params['aggregation.fold'] = aggregation
      Cursor.new(session.build_uri(url, params), session, DataCursor, DataSet)
    end

    def read_multi(start, stop, options = {})
      params = rollup_params(attribute_params(options))
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ["multi"]
      Cursor.new(session.build_uri(url, params), session, DataCursor, MultiPointSegment, 'series')
    end

    def write_data(series_key, data)
      url = ['series', 'key', series_key, 'data']
      body = data.collect {|dp| dp.to_json()}
      session.do_post(url, nil, body)
    end

    def write_multi(multi = nil)
      req = multi || MultiWrite.new
      if block_given?
        yield req
      elsif multi.nil?
        raise TempoDBClientError.new("You must either pass a multi write object, or provide a block")
      end

      session.do_post(['multi'], nil, JSON.generate(req.series))
    end
    
    def single_value(key, options = {})
      params = single_value_params(options)
      params['ts'] = params['ts'].iso8601(3) if params['ts']
      json = session.do_get(['series', 'key', key, 'single'], single_value_params(params))
      SingleValue.from_json(json)
    end

    def multi_series_single_value(options = {})
      params = single_value_params(attribute_params(options))
      params['ts'] = params['ts'].iso8601(3) if params['ts']
      Cursor.new(session.build_uri(['single'], params), session, ArrayCursor, SingleValue)
    end

    def delete_data(series_key, start, stop)
      params = {}
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'data']
      session.do_delete(url, params)
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
                 :rollup_functions => 'rollup.fold',
                 :rollup_period => 'rollup.period',
                 :interpolation_function => 'interpolation.function',
                 :interpolation_period => 'interpolation.period')
    end

    def find_params(params)
      map_params(params,
                 :predicate_function => 'predicate.function',
                 :predicate_period => 'predicate.period')
    end

    def single_value_params(params)
      map_params(params,
                 :ts => 'ts',
                 :direction => 'direction')
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

