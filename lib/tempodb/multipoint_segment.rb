module TempoDB
  # A logical page of MultiPoint with additional metadata
  class MultiPointSegment
    attr_accessor :data, :rollup, :tz

    def initialize(data, rollup, tz)
      @data = data
      @rollup = rollup
      @tz = tz
    end

    def self.from_json(m)
      data = m["data"].map { |mdp| MultiPoint.from_json(mdp) }
      rollup = m["rollup"]
      tz = m["tz"]
      new(data, rollup, tz)
    end
  end
end

