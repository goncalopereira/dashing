require 'net/http'
require 'nokogiri'
# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '1m', :first_in => 0 do |job|

  l = logs

  u = parse_url l

  p = parameters u

  trackid = p["trackId"]
  country = p["country"]

  if country.nil? 
    country = "GB"
  end

  xml = track_details_xml trackid, country

  track = track_details xml

  send_event('artwork', { image: track["artwork"], width: 350 })
  send_event('name', {title: track["artist_name"], text: track["track_name"], moreinfo: track["release_name"]})

  c = consumers l
  send_event('consumers', { items: c.values })
end

def consumers logs
  consumers = Hash.new({ value: 0 })
  consumers["test"] = { label: "test", value: 1 }
  results = JSON.parse(logs)["hits"]["hits"]
  
  puts results

  consumers["total"] = { label: "total", value: results.length } 

  consumers
end

def logs
  
  #previous minute
  time = (Time.now - 60)
  timestamp  = time.to_i * 1000
  parsed_month = "%02d" % time.month
  parsed_day = "%02d" % time.day
 
  puts time
  index = "logstash-"+time.year.to_s+"." + parsed_month + "." + parsed_day

  uri = URI("http://logginges.prod.svc.7d:9200/"+ index +"/_search?pretty")
  http = Net::HTTP.new(uri.host, uri.port)

  request = Net::HTTP::Post.new(uri.request_uri)

 request.body = '{
"query": {
"filtered": {
"query": {
"bool": {
"should": [
{
"query_string": {
"query": "@endpoint_id == 1850"
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

  puts request.body
  response =  http.request(request) 
  response.body
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
