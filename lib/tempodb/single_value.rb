module TempoDB
  class SingleValue
    attr_accessor :series, :data

    def initialize(series, data)
      @series = series
      @data = data
    end

    def self.from_json(json)
      new(Series.from_json(json["series"]), DataPoint.from_json(json["data"]))
    end
  end
end
