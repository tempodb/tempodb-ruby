require 'spec_helper'

describe TempoDB::Client do
  it "should not throw an exception when using SSL" do
    stub_request(:get, "https://api.tempo-db.com/v1/series/?key=my_key").
      to_return(:status => 200, :body => "{}", :headers => {})
    client = TempoDB::Client.new("key", "secret")
    client.get_series(:keys => "my_key")
  end

  describe "get_series" do
    context "with no options provided" do
      it "lists all series in the database" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?").
          to_return(:status => 200, :body => response_fixture('list_all_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        client.get_series.size.should == 7
      end
    end

    context "with filter options provided" do
      it "lists all series that meet the filtered criteria" do
        stub_request(:get, "https://api.tempo-db.com/v1/series/?key=key1&key=key2").
          to_return(:status => 200, :body => response_fixture('list_filtered_series.json'), :headers => {})
        client = TempoDB::Client.new("key", "secret")
        series = client.get_series(:keys => ["key1", "key2"])
        series.map(&:key).should == ["key1", "key2"]
      end
    end
  end
end
