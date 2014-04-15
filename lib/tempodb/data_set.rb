module TempoDB
  class DataSet
    attr_accessor :series, :data, :rollup, :tz

    def initialize(series, data, rollup, tz)
      @series = series
      @data = data
      @rollup = rollup
      @tz = tz
    end

    def self.from_json(m)
      series = Series.from_json(m["series"]) if m["series"]
      data = m["data"].map { |dp| DataPoint.from_json(dp)}
      rollup = m["rollup"]
      tz = m["tz"]
      new(series, data, rollup, tz)
    end
  end
end

