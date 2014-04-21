module TempoDB
  # Returned from 'delete' calls.
  #
  # * +deleted+ [Integer] - The number of items deleted
  class DeleteSummary
    attr_reader :deleted

    def initialize(deleted)
      @deleted = deleted
    end

    def self.from_json(hash)
      new(hash["deleted"])
    end
  end
end

