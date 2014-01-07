require 'net/http'
require 'nokogiri'
# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '1m', :first_in => 0 do |job|

  subscriptions = logs 1850
  catalogue = logs 1830
  locker = logs 1870

  u = parse_url subscriptions

  p = parameters u

  puts p

  trackid = p["trackId"]
  country = "GB"

  if !p["country"].nil? 
    country = p["country"]
  end

  track = nil
  begin
    xml = track_details_xml trackid, country
    track = track_details xml
  rescue StandardError
    track = Hash.new
    track["track_name"] = "Track Details Error"
  end

  send_event('artwork', { image: track["artwork"], width: 350 })
  send_event('name', {title: track["artist_name"], text: track["track_name"], moreinfo: track["release_name"]})

  c = consumers subscriptions
  send_event('consumers_subscription', { items: c.values })
  c = consumers catalogue
  send_event('consumers_catalogue', { items: c.values })
  c = consumers locker
  send_event('consumers_locker', { items: c.values })
end

def consumers logs
  consumers = Hash.new({ value: 0 })
  results = parse_consumers logs
  
  puts results
   
  results.keys.each do |key|
    consumers[key] = { label: key[0..15], value: results[key] }
  end
  consumers
end

def es_query endpointid, timestamp
'{
"query": {
"filtered": {
"query": {
"bool": {
"should": [
{
"query_string": {
"query": "@endpoint_id == '+ endpointid.to_s + '"
}
}
]
}
},
"filter": {
"bool": {
"must": [
{
"match_all": {}
},
{
"range": {
"@timestamp": {
"from": '+ timestamp.to_s + ',
"to": "now"
}
}
},
{
"bool": {
"must": [
{
"match_all": {}
}
]
}
}
]
}
}
}
},
"size": 500,
"sort": [
{
"@timestamp": {
"order": "desc"
}
}
]
}'
end

def logs endpoint_id
  
  #previous minute
  time = (Time.now - 60)
  timestamp  = time.to_i * 1000
  parsed_month = "%02d" % time.month
  parsed_day = "%02d" % time.day
 
  index = "logstash-"+time.year.to_s+"." + parsed_month + "." + parsed_day

  uri = URI("http://logginges.prod.svc.7d:9200/"+ index +"/_search")
  http = Net::HTTP.new(uri.host, uri.port)

  request = Net::HTTP::Post.new(uri.request_uri)

  request.body = es_query endpoint_id, timestamp

  response =  http.request(request) 
  response.body
end

def parse_consumers response_body
  results = Hash.new
  list = JSON.parse(response_body)["hits"]["hits"]
  
  list.each do |hit|
    if results[hit["_source"]["consumer_name"]].nil?
      results[hit["_source"]["consumer_name"]] = 1
    else
      results[hit["_source"]["consumer_name"]] = results[hit["_source"]["consumer_name"]]+1
    end
  end

  results
end

def parse_url response_body
  JSON.parse(response_body)["hits"]["hits"][0]["_source"]["url"]
end

def parameters u
  uri = URI(u)
  Rack::Utils.parse_nested_query uri.query
end

def track_details_xml trackid, country
  track_details_url = "http://api.7digital.com/1.2/track/details?oauth_consumer_key=test-api&trackId="+trackid+"&country="+country
  puts track_details_url
  response = Net::HTTP.get_response(URI(track_details_url))
  Nokogiri::XML(response.body) 
end

def track_details xml 
  track = Hash.new 

  track["artist_name"] = xml.at_xpath("//response/track/artist/name").content
  track["track_name"] = xml.at_xpath("//response/track/title").content
  track["release_name"] = xml.at_xpath("//response/track/release/title").content

  small_artwork = xml.at_xpath("//response/track/release/image").content
  track["artwork"] = small_artwork.sub "_50", "_350"

  track
end
