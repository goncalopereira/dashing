require 'net/http'
require 'nokogiri'
# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '3m', :first_in => 0 do |job|

  eu_uri = "http://prod-mediadelivery-monitoring00.nix.sys.7d/render?target=stats.timers.Prod.Streaming.Catalogue.Cached.External.EU.TTFB.mean&format=json&from=-10minutes"  
  use_uri = "http://prod-mediadelivery-monitoring00.nix.sys.7d/render?target=stats.timers.Prod.Streaming.Catalogue.Cached.External.USE.TTFB.mean&format=json&from=-10minutes"

  send_event('ttfb_eu', { value: (graphite_datapoints eu_uri)[0] })
  send_event('ttfb_use', { value: (graphite_datapoints use_uri)[0] })
end

def graphite_datapoints url 

  response = Net::HTTP.get_response(URI(url))
 
  json = JSON.parse(response.body) 

  json[0]["datapoints"].each do |datapoint|
    if not datapoint[0].nil?
      return datapoint
    end
  end
  return [nil,0]
end
