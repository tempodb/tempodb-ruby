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

  describe "read_key" do
    it "has an array of DataPoints" do
      start = Time.parse("2012-01-01")
      stop = Time.parse("2012-01-02")
      stub_request(:get, "https://api.tempo-db.com/v1/series/key/key1/data/?end=2012-01-02T00:00:00.000-06:00&function=&interval=&start=2012-01-01T00:00:00.000-06:00&tz=").
        to_return(:status => 200, :body => response_fixture('read_key.json'), :headers => {})
      client = TempoDB::Client.new("key", "secret")
      set = client.read_key("key1", start, stop)
      set.data.all? { |d| d.is_a?(TempoDB::DataPoint) }.should be_true
      set.data.size.should == 1440
    end
  end
end
