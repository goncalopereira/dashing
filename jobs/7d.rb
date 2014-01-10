require 'net/http'
require 'nokogiri'
# :first_in sets how long it takes before the job is first run. In this case, it is run immediately
SCHEDULER.every '1m', :first_in => 0 do |job|

  l = Hash.new
  l["StreamSubscription"] = logs 1850, 0, 60
  l["Catalogue"] = logs 1830, 0, 60
  l["StreamLocker"] = logs 1870, 0, 60

  select = l.keys.sample

  u = parse_field "url", l[select]
  p = parameters u[0]

  cn = (parse_field "consumer_name", l[select])[0]

  uid = (parse_field "user_id", l[select])[0]
  
  hourly_streams select, uid

  trackid = p["trackid"]
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

  text = "\"#{track["track_name"]}\" by \"#{track["artist_name"]}\" on \"#{track["release_name"]}\" listened from \"#{country}\" with \"#{cn}\""
  send_event('artwork', { image: track["artwork"], width: 280 })
  send_event('name', { title: select, text: text})
 
  c = event_list (parse_consumers l["StreamSubscription"])
  send_event('consumers_subscription', { items: c.values })
  c = event_list (parse_consumers l["Catalogue"])
  send_event('consumers_catalogue', { items: c.values })
  c = event_list (parse_consumers l["StreamLocker"])
  send_event('consumers_locker', { items: c.values })

  u = parse_field "url", l["StreamSubscription"]
  countries = parse_countries u, Hash.new

  u = parse_field "url", l["Catalogue"]
  countries = parse_countries u, countries

  u = parse_field "url", l["StreamLocker"]
  c = event_list (parse_countries u, countries)
  send_event('countries', { items: c.values })

  media_speed select, country
end

def hourly_streams select, uid
  u_logs = []
  if select == "StreamSubscription" 
    response_body = logs 1850, uid, 3600
    u_logs = JSON.parse(response_body)["hits"]["hits"]
    send_event("hourly_streams", { value: u_logs.length })
  end

  if select == "StreamLocker"
    response_body = logs 1870, uid, 3600
    u_logs = JSON.parse(response_body)["hits"]["hits"]    
    send_event("hourly_streams", { value: u_logs.length })
  end

  send_event("hourly_streams", { value: u_logs.length }) 
end

def event_list results
  consumers = Hash.new({ value: 0 })
   
  results.keys.each do |key|
    consumers[key] = { label: key[0..15], value: results[key] }
  end
  
  consumers
end

def es_query timestamp, query
'{
"query": {
"filtered": {
"query": {
"bool": {
"should": [
{
"query_string": {
"query": "' + query + '"
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

def logs endpoint_id, user_id, seconds
  
  #previous minute
  time = (Time.now - seconds)
  timestamp  = time.to_i * 1000
  parsed_month = "%02d" % time.month
  parsed_day = "%02d" % time.day
 
  index = "logstash-"+time.year.to_s+"." + parsed_month + "." + parsed_day

  uri = URI("http://logginges.prod.svc.7d:9200/"+ index +"/_search")
  http = Net::HTTP.new(uri.host, uri.port)

  request = Net::HTTP::Post.new(uri.request_uri)

  query =  "endpoint_id: #{endpoint_id}"

  if user_id != 0
    query = query + " AND user_id: #{user_id}"
  end

  request.body = es_query timestamp, query

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

def parse_countries urls, results

  urls.each do |url|
    uri = URI(url)
    q = Rack::Utils.parse_nested_query uri.query

    country = q["country"]

    if country.nil?
      country = "GB"
    end

    if results[country].nil?
      results[country] = 0
    end
  
    results[country] = results[country]+1
  end

  results
end

def parse_field field, response_body
  results = []
  JSON.parse(response_body)["hits"]["hits"].each do |hit|
    results << hit["_source"][field]
  end
  results
end

def parameters u
  uri = URI(u)
  q = Rack::Utils.parse_nested_query uri.query
  downcased = Hash.new
  q.each do |k,v|
    downcased[k.downcase] = v
  end
  downcased
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


def media_speed media_type, country
    url = nil
    location = nil

    if country == "US"
      location = "USE"
    else
      location = "EU"
    end

    url = "http://prod-mediadelivery-monitoring00.nix.sys.7d/render?target=stats.timers.Prod.Streaming.#{media_type}.Cached.External.#{location}.TTFB.mean&format=json&from=-10minutes"  
    send_event('ttfb', { value: (graphite_datapoints url)[0], title: "Cached TTFB from #{location}" })
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


