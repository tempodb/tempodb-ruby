require 'enumerator'
require 'httpclient_link_header'

module TempoDB
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

  class DataCursor
    def self.extract(data_set)
      data_set.data
    end
  end

  class ArrayCursor
    def self.extract(elems)
      elems
    end
  end
end
