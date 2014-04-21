require 'enumerator'
require 'httpclient_link_header'

module TempoDB
  # A Cursor represents the fundamental way in which large streams of data are
  # read from the TempoDB API. Most read calls that return large collections of
  # objects will return a Cursor. A Cursor represents one logical read of a query,
  # but might generate multiple HTTP calls lazily upon iteration. You can think of
  # a cursor as handling the 'pagination' problem, without having to explicitly think
  # about pages in your application.
  #
  # ==== Usage
  #
  # A Cursor implements the Ruby Enumerable interface, and can thus be iterated over just
  # like you might iterate over any other collection type:
  #
  #    cursor = client.read_data('temp-1', start, stop)
  #    cursor.each do |datapoint|
  #      puts "#{datapoint.ts}: #{datapoint.value}"
  #    end
  #
  # If you know you are working with large datasets, lazy iteration will give you
  # the best memory performance. On the other hand, if you are working with small
  # collections, it might be convenient to work with arrays directly:
  #
  #    datapoints = client.read_data('temp-1', start, stop).to_a
  #    puts "Total datapoints returned: #{datapoints.size}"
  #
  # Remember that a Cursor may make many roundtrips to the server, depending on
  # how much data you request in your query.
  class Cursor
    attr_reader :session

    include Enumerable

    TRUNCATED_KEY = "Truncated"

    def initialize(uri, session, inner_cursor_type, wrapper, *extra_attributes)
      @uri = uri
      @next_uri = uri
      @session = session
      @wrapper = wrapper
      @inner_cursor_type = inner_cursor_type
      @extra_attributes = extra_attributes
      @attributes = {}
      @segment = nil
    end

    def [](key)
      get_segment!

      @attributes[key]
    end

    def each
      # The cursor has been completely consumed
      return if @segment && @segment.empty? && !@next_uri

      loop do
        get_segment!

        # Consume the segment
        @segment.size.times do
          yield @segment.pop
        end

        # Break because there're no more pages to fetch
        return unless @next_uri
      end
    end

    private

    def get_segment!
      if @segment == nil || @segment.empty?
        response = session.cursored_get(@next_uri)
        check_response(response)
        json = JSON.parse(response.body)
        wrapped = json.is_a?(Array) ? json.map { |obj| @wrapper.from_json(obj) } : @wrapper.from_json(json)
        @extra_attributes.each { |attr| @attributes[attr] = json[attr] }
        @segment = @inner_cursor_type.extract(wrapped).reverse
        if !response.header[TRUNCATED_KEY].empty?
          @next_uri = URI(session.construct_uri(response.links.by("rel").fetch("next").first["uri"]).to_s.chomp("/"))
        else
          # We're at the last page
          @next_uri = nil
        end
      end
    end

    def check_response(response)
      unless response.code == HTTP::Status::OK
        raise TempoDBClientError.new("TempoDB API returned #{response.code} as status when 200 was expected: #{response.body}", response)
      end
    end
  end

  # Cursor through responses that return an array in the +data+ field.
  class DataCursor
    def self.extract(data_set)
      data_set.data
    end
  end

  # Cursor through responses that return a top-level Array
  class ArrayCursor
    def self.extract(elems)
      elems
    end
  end
end
