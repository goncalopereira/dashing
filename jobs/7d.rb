require 'net/http'
require 'nokogiri'
# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '1m', :first_in => 0 do |job|

  response_body = '{
  "took" : 17,
  "timed_out" : false,
  "_shards" : {
  "total" : 1,
  "successful" : 1,
  "failed" : 0
  },
  "hits" : {
  "total" : 1338,
  "max_score" : null,
  "hits" : [ {
  "_index" : "logstash-2014.01.03",
  "_type" : "api-request-log",
  "_id" : "VqSBe5HkTu6u_RWgilXkhg",
  "_score" : null, "_source" : {"consumer_id":155,"consumer_key":"pure","consumer_name":"Pure Live (Web)","partner_id":849,"endpoint_id":1850,"endpoint":"~/stream/subscription","url":"http://stream.svc.7digital.net/stream/subscription?clientId=94be9733-e772-479c-a45d-5b1ab9a2a148&formatId=26&oauth_consumer_key=pure&oauth_nonce=6401494&oauth_signature_method=HMAC-SHA1&oauth_timestamp=1388771210&oauth_version=1.0&releaseId=630873&shopId=34&trackId=7001800&userId=94be9733-e772-479c-a45d-5b1ab9a2a148&oauth_signature=GQhkIPfZS7SvxP5SlZKaosSr4DY=","consumer_ip":"83.244.128.126","verb":"GET","user_id":122866878,"shop_id":34,"server":"PROD-APIWEB08","response_time":15,"cached":false,"internal_status":0,"@timestamp":"2014-01-03T17:46:49.869Z","from_user_auth":true,"@version":"1","type":"api-request-log"},
  "sort" : [ 1388771209869 ]
  }]
  }
  }'

  logs

  u = parse_url response_body

  p = parameters u

  trackid = p["trackId"]

  xml = track_details_xml trackid

  track = track_details xml

  send_event('artwork', { image: track["artwork"], width: 350 })
  send_event('name', {title: track["artist_name"], text: track["track_name"], moreinfo: track["release_name"]})

  consumers = Hash.new({ value: 0 })
  consumers["test"] = { label: "test", value: 1 }
  send_event('consumers', { items: consumers.values })
end

def logs
  
  #previous minute
  time = (Time.now - 60)
  timestamp  = time.to_i
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
"query": "@endpoint_id == 1850 "
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
"highlight": {
"fields": {},
"fragment_size": 2147483647,
"pre_tags": [
"@start-highlight@"
],
"post_tags": [
"@end-highlight@"
]
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
#  puts response.body
end

def parse_url response_body
  JSON.parse(response_body)["hits"]["hits"][0]["_source"]["url"]
end

def parameters u
  uri = URI(u)
  Rack::Utils.parse_nested_query uri.query
end

def track_details_xml trackid
  response = Net::HTTP.get_response(URI("http://api.7digital.com/1.2/track/details?oauth_consumer_key=test-api&trackId="+trackid))
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
