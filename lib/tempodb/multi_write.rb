module TempoDB
  class MultiWrite
    attr_reader :series

    def initialize
      @series = []
    end

    def add(series_key, data)
      data.each do |series|
        @series << { "key" => series_key }.merge(series.to_json)
      end
    end
  end
end
