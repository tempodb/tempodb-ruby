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

    # Create a TempoDB API Client
    #
    # * +database_id+ [String] - Your TempoDB database_id
    # * +key+ [String] - TempoDB REST API key
    # * +host+ (optional) [String] - REST API Host
    # * +port+ (optional) [Integer] - REST API port
    # * +secure+ (optional) [TrueClass | FalseClass] - Defaults to true
    # 
    # returns Client
    def initialize(database_id, key, secret, host = TempoDB::API_HOST, port = TempoDB::API_PORT, secure = true)
      @database_id = database_id
      @session = Session.new(key, secret, host, port, secure)
    end

    # Create a new time series with an optional series key.
    #
    # * +series_key+ [String] - The series key string
    # * +options+ [Hash] - Optional series create parameters
    #
    # returns Series on success, raises TempoDBClientError on failure
    #
    # ==== Options
    # 
    # * +:tags+ [Array] - An array of series tags. Each tag should be a string.
    # * +:attributes+ [Hash] - A hash of series attributes. Keys/Values are strings
    # 
    # ==== Example
    #    series = client.create_series('temp-1', :tags => ['foo', 'bar'],
    #                                            :attributes => {'baz' => 'boz'})
    def create_series(series_key = nil, options = {})
      params = options
      params = params.merge('key' => series_key) if series_key
      json = session.do_post(['series'], nil, params)
      Series.from_json(json)
    end

    # Delete Series object(s) from a database
    #
    # * +options+ [Hash] - Filter criteria with which to delete
    #
    # returns DeleteSummary on success, raises TempoDBClientError on
    # failure
    #
    # ==== Options
    #
    # * +:tags+ [Array] - An array of series tags. Each tag should be a string.
    # * +:attributes+ [Hash] - A hash of series attributes. Keys/Values are strings
    # * +:keys+ [Array] - An array of series keys. Keys are strings.
    # 
    # ==== Example
    #    summary = client.delete_series(:attributes => {"building_number" => "321"})
    #    puts "Number of series deleted: #{summary.deleted}"
    #
    def delete_series(params)
      json = session.do_delete(['series'], attribute_params(params))
      DeleteSummary.from_json(json)
    end

    # Fetch a Series by key
    #
    # * +series_key+ [String] - The series key
    #
    # returns Series on success, raises TempoDBClientError on failure
    #
    # ==== Example
    #
    #    series = client.get_series('temp-1')
    #    puts "Series key: #{series.key}"
    #
    def get_series(series_key)
      json = session.do_get(['series', 'key', series_key])
      Series.from_json(json)
    end

    # List Series by the given filter criteria
    #
    # * +options+ [Hash] - Filter criteria with which to search
    #
    # returns Cursor of Series on success, raises TempoDBClientError on
    # failure
    #
    # ==== Options
    #
    # * +:tags+ [Array] - An array of series tags. Each tag should be a string.
    # * +:attributes+ [Hash] - A hash of series attributes. Keys/Values are strings
    # * +:keys+ [Array] - An array of series keys. Keys are strings.
    # 
    # ==== Example
    # 
    #    cursor = client.list_series(:tags => ['foo', 'bar'])
    #    cursor.each do |series|
    #      puts "Series: #{series.key}"
    #    end
    def list_series(options = {})
      Cursor.new(session.build_uri(['series'], attribute_params(options)), session, ArrayCursor, Series)
    end

    # Update the metadata on a Series
    #
    # * +series+ [Series] - The updated Series object.
    #
    # returns Series on success, raises TempoDBClientError on failure
    #
    # ==== Example
    #
    #    series = client.get_series('temp-1')
    #    series.attributes['foo'] = 'bar'
    #    client.update_series(series)
    #
    def update_series(series)
      json = session.do_put(['series', 'id', series.id], nil, series.to_json)
      Series.from_json(json)
    end

    # Read datapoints from a specific series
    #
    # * +series_key+ [String] - The series key to read from
    # * +start+ [Time] - The start time to begin reading from
    # * +stop+ [Time] - The end time to stop reading from
    # * +options+ [Hash] - Additional read options
    #
    # returns Cursor of DataPoint on succes, raises TempoDBClientError on failure
    #
    # ==== Options
    # * +:rollup_function+ [String] - A rollup function to apply to the datapoints. One of:
    #   * count - The number of datapoints in the :rollup_period
    #   * sum - Summation of all datapoint values in the :rollup_period
    #   * mult - Multiplication of all datapoint values in the :rollup_period
    #   * min - The smallest datapoint value in the :rollup_period
    #   * max - The largest datapoint value in the :rollup_period
    #   * stddev - The standard deviation of the datapoint values in the :rollup_period
    #   * ss - Sum of squares of all datapoint values in the :rollup_period
    #   * range - The maximum value less the minimum value of the datapoint values in the :rollup_period
    #   * percentile,N (where N is what percentile to calculate) - Percentile of datapoint values in :rollup_period
    # * +rollup_period+ [String] - The duration of each rollup. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    # * +:interpolation_function+ [String] - The type of interpolation to perform. One of:
    #   * linear - Perform linear interpolation
    #   * zoh - Zero order hold interpolation
    # * +:interpolation_period [String] - The sampling rate to interpolate datapoints. Should always be smaller than :rollup_period. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    #
    # ==== Example
    #
    # Find the average of all datapoints between start and stop for each hour,
    # with datapoints sampled at 1 minute intervals:
    #
    #    start = Time.utc(2012, 1, 1)
    #    stop = Time.utc(2012, 1, 2)
    #    cursor = client.read_data('temp-1', start, stop,
    #                              :rollup_function => "avg", :rollup_period => "1hour"
    #                              :interpolation_function => "linear", :interpolation_period => "1min")
    #    cursor.each do |datapoint|
    #      puts "#{datapoint.ts}: #{datapoint.value}"
    #    end
    #
    def read_data(series_key, start, stop, options = {})
      params = rollup_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'segment']
      Cursor.new(session.build_uri(url, params), session, DataCursor, DataSet, 'start', 'end', 'rollup', 'series')
    end

    # Retrieve the series Summary for the given series key
    #
    # * +series_key+ [String] - The series key to read from
    # * +start+ [Time] - The start time to begin reading from
    # * +stop+ [Time] - The end time to stop reading from
    # * +options+ [Hash] - Additional read options
    #
    # returns a SeriesSummary on success, raises TempoDBClientError on failure
    #
    # ==== Options
    # * +:rollup_function+ [String] - A rollup function to apply to the datapoints. One of:
    #   * count - The number of datapoints in the :rollup_period
    #   * sum - Summation of all datapoint values in the :rollup_period
    #   * mult - Multiplication of all datapoint values in the :rollup_period
    #   * min - The smallest datapoint value in the :rollup_period
    #   * max - The largest datapoint value in the :rollup_period
    #   * stddev - The standard deviation of the datapoint values in the :rollup_period
    #   * ss - Sum of squares of all datapoint values in the :rollup_period
    #   * range - The maximum value less the minimum value of the datapoint values in the :rollup_period
    #   * percentile,N (where N is what percentile to calculate) - Percentile of datapoint values in :rollup_period
    # * +rollup_period+ [String] - The duration of each rollup. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    # * +:interpolation_function+ [String] - The type of interpolation to perform. One of:
    #   * linear - Perform linear interpolation
    #   * zoh - Zero order hold interpolation
    # * +:interpolation_period [String] - The sampling rate to interpolate datapoints. Should always be smaller than :rollup_period. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    #
    # ==== Example
    #
    #    start = Time.utc(2012, 1, 1)
    #    stop = Time.utc(2012, 1, 2)
    #    cursor = client.get_summary('temp-1', start, stop)
    #
    def get_summary(series_key, start, stop, options = {})
      params = rollup_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      json = session.do_get(["series", "key", series_key, "summary"], params)
      SeriesSummary.from_json(json)
    end

    # Read multiple rollups for a single series
    #
    # * +series_key+ [String] - The series key to read from
    # * +start+ [Time] - The start time to begin reading from
    # * +stop+ [Time] - The end time to stop reading from
    # * +options+ [Hash] - Additional read options
    #
    # returns a Cursor of MultiPoint on success, raises TempoDBClientError on failure
    #
    # ==== Options
    # * +:rollup_functions+ [Array] - An array of rollup functions to apply to the datapoints. One of:
    #   * count - The number of datapoints in the :rollup_period
    #   * sum - Summation of all datapoint values in the :rollup_period
    #   * mult - Multiplication of all datapoint values in the :rollup_period
    #   * min - The smallest datapoint value in the :rollup_period
    #   * max - The largest datapoint value in the :rollup_period
    #   * stddev - The standard deviation of the datapoint values in the :rollup_period
    #   * ss - Sum of squares of all datapoint values in the :rollup_period
    #   * range - The maximum value less the minimum value of the datapoint values in the :rollup_period
    #   * percentile,N (where N is what percentile to calculate) - Percentile of datapoint values in :rollup_period
    # * +:rollup_period+ [String] - The duration of each rollup. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    # * +:interpolation_function+ [String] - The type of interpolation to perform. One of:
    #   * linear - Perform linear interpolation
    #   * zoh - Zero order hold interpolation
    # * +:interpolation_period [String] - The sampling rate to interpolate datapoints. Should always be smaller than :rollup_period. Specified by:
    #   * A number and unit of time: EG - '1min' '10days'.
    #   * A valid ISO8601 duration
    #
    # ==== Example
    #
    #    cursor = client.read_multi_rollups('temp-1', start, stop,
    #                                       :rollup_functions => ['max', 'min', 'count'],
    #                                       :rollup_period => '1hour')
    #    
    #    cursor.each do |datapoint|
    #      maxv = datapoint.value['max']
    #      minv = datapoint.value['min']
    #      count = datapoint.value['count']
    #      puts "#{datapoint.ts}: max: #{maxv}, min: #{minv}, count: #{count}"
    #    end
    # 
    def read_multi_rollups(series_key, start, stop, options = {})
      params = rollup_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'data', 'rollups', 'segment']
      Cursor.new(session.build_uri(url, params), session, DataCursor, MultiPointSegment, 'series')
    end

    # Finds data from a given series_key according to a specified predicate function
    #
    # * +series_key+ [String] - The series key to read from
    # * +start+ [Time] - The start time to begin reading from
    # * +stop+ [Time] - The end time to stop reading from
    # * +options+ [Hash] - Additional read options
    #
    # return Cursor of DataPointFound on success, raises TempoDBClientError on failure
    def find_data(series_key, start, stop, options = {})
      params = find_params(options)
      params['start'] = start.iso8601(3)
      params['end'] = stop.iso8601(3)
      url = ['series', 'key', series_key, 'find']
      Cursor.new(session.build_uri(url, params), session, DataCursor, DataPointFound, 'find', 'predicate')
    end

    def aggregate_data(start, stop, aggregation, options = {})
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
      body = data.collect { |dp| dp.to_json }
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

