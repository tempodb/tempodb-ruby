require 'spec_helper'

describe TempoDB do
  describe "client" do
    it "should not throw an exception when using SSL" do
      stub_request(:get, "https://key:secret@api.tempo-db.com/v1/series/?key=my_key").
        to_return(:status => 200, :body => "{}", :headers => {})
      client = TempoDB::Client.new("key", "secret")
      client.get_series(:keys => "my_key")
    end
  end
end

