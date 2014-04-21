module TempoDB
  # Represents one logical stream of time series data
  class Series
    attr_accessor :id, :key, :name, :attributes, :tags

    def initialize(id, key, name="", attributes={}, tags=[])
      @id = id
      @key = key
      @name = name
      @attributes = attributes
      @tags = tags
    end

    def to_json(*a)
      { "id" => id, "key" => key, "name" => name, "attributes" => attributes, "tags" => tags }.to_json(*a)
    end

    def self.from_json(m)
      new(m["id"], m["key"], m["name"], m["attributes"], m["tags"])
    end
  end
end

