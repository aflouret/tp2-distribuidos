import os
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read("config.ini")

rabbitmq_connection_string = config["DEFAULT"]["RABBITMQ_CONNECTION_STRING"]

client_handler_instances = 1
client_instances = int(config["DEFAULT"]["CLIENT_INSTANCES"])
data_dropper_instances = int(config["DEFAULT"]["DATA_DROPPER_INSTANCES"])

weather_joiner_instances = int(config["DEFAULT"]["WEATHER_JOINER_INSTANCES"])
precipitation_filter_instances = int(config["DEFAULT"]["PRECIPITATION_FILTER_INSTANCES"])
duration_averager_instances = int(config["DEFAULT"]["DURATION_AVERAGER_INSTANCES"])

stations_joiner_instances = int(config["DEFAULT"]["STATIONS_JOINER_INSTANCES"])
year_filter_instances = int(config["DEFAULT"]["YEAR_FILTER_INSTANCES"])
trip_counter_instances= int(config["DEFAULT"]["TRIP_COUNTER_INSTANCES"])

distance_calculator_instances = int(config["DEFAULT"]["DISTANCE_CALCULATOR_INSTANCES"])
distance_averager_instances = int(config["DEFAULT"]["DISTANCE_AVERAGER_INSTANCES"])

duration_merger_instances = 1
count_merger_instances = 1
distance_merger_instances = 1
merger_instances = duration_merger_instances + count_merger_instances + distance_merger_instances

year_1 = int(config["DEFAULT"]["YEAR_1"])
year_2 = int(config["DEFAULT"]["YEAR_2"])

minimum_distance = config["DEFAULT"]["MIN_DISTANCE"]
minimum_precipitations = config["DEFAULT"]["MIN_PRECIPITATIONS"]

client_string = ""
for i in range(0, client_instances):
    client_string = client_string + f'''
  client_{i}:
    container_name: client_{i}
    image: client:latest
    entrypoint: /client
    restart: on-failure
    depends_on:
      - client_handler
    volumes:
      - type: bind
        source: ./data
        target: /data
      - type: bind
        source: ./client/config.yaml
        target: /config.yaml
'''

data_dropper_string = ""
for i in range(0, data_dropper_instances):
    data_dropper_string = data_dropper_string + f'''
  data_dropper_{i}:
    container_name: data_dropper_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={client_handler_instances}
      - WEATHER_JOINER_INSTANCES={weather_joiner_instances}
      - STATIONS_JOINER_INSTANCES={stations_joiner_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: data_dropper:latest
    entrypoint: /data_dropper
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./data_dropper/middleware_config.yaml
        target: /middleware_config.yaml
'''


weather_joiner_string = ""
for i in range(0, weather_joiner_instances):
    weather_joiner_string = weather_joiner_string + f'''
  weather_joiner_{i}:
    container_name: weather_joiner_{i}
    environment:
      - ID={i}
      - CLIENT_HANDLER_INSTANCES={client_handler_instances}
      - DATA_DROPPER_INSTANCES={data_dropper_instances}
      - NEXT_STAGE_INSTANCES={precipitation_filter_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: weather_joiner:latest
    entrypoint: /weather_joiner
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./weather_joiner/middleware_config.yaml
        target: /middleware_config.yaml
''' 


stations_joiner_string = ""
for i in range(0, stations_joiner_instances):
    stations_joiner_string = stations_joiner_string + f'''
  stations_joiner_{i}:
    container_name: stations_joiner_{i}
    environment:
      - ID={i}
      - CLIENT_HANDLER_INSTANCES={client_handler_instances}
      - DATA_DROPPER_INSTANCES={data_dropper_instances}
      - YEAR_FILTER_INSTANCES={year_filter_instances}
      - DISTANCE_CALCULATOR_INSTANCES={distance_calculator_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: stations_joiner:latest
    entrypoint: /stations_joiner
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./stations_joiner/middleware_config.yaml
        target: /middleware_config.yaml
''' 

precipitation_filter_string = ""
for i in range(0, precipitation_filter_instances):
    precipitation_filter_string = precipitation_filter_string + f'''
  precipitation_filter_{i}:
    container_name: precipitation_filter_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={weather_joiner_instances}
      - NEXT_STAGE_INSTANCES={duration_averager_instances}
      - MIN_PRECIPITATIONS={minimum_precipitations}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: precipitation_filter:latest
    entrypoint: /precipitation_filter
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./precipitation_filter/middleware_config.yaml
        target: /middleware_config.yaml
'''   

distance_calculator_string = ""
for i in range(0, distance_calculator_instances):
    distance_calculator_string = distance_calculator_string + f'''
  distance_calculator_{i}:
    container_name: distance_calculator_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={stations_joiner_instances}
      - NEXT_STAGE_INSTANCES={distance_averager_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: distance_calculator:latest
    entrypoint: /distance_calculator
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./distance_calculator/middleware_config.yaml
        target: /middleware_config.yaml
''' 

duration_averager_string = ""
for i in range(0, duration_averager_instances):
    duration_averager_string = duration_averager_string + f'''
  duration_averager_{i}:
    container_name: duration_averager_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={precipitation_filter_instances}
      - NEXT_STAGE_INSTANCES=1
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: duration_averager:latest
    entrypoint: /duration_averager
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./duration_averager/middleware_config.yaml
        target: /middleware_config.yaml
'''   

distance_averager_string = ""
for i in range(0, distance_averager_instances):
    distance_averager_string = distance_averager_string + f'''
  distance_averager_{i}:
    container_name: distance_averager_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={distance_calculator_instances}
      - NEXT_STAGE_INSTANCES=1
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: distance_averager:latest
    entrypoint: /distance_averager
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./distance_averager/middleware_config.yaml
        target: /middleware_config.yaml
'''   

year_filter_string = ""
for i in range(0, year_filter_instances):
    year_filter_string = year_filter_string + f'''
  year_filter_{i}:
    container_name: year_filter_{i}
    environment:
      - ID={i}
      - PREV_STAGE_INSTANCES={stations_joiner_instances}
      - NEXT_STAGE_INSTANCES={trip_counter_instances}
      - YEAR_1={year_1}
      - YEAR_2={year_2}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: year_filter:latest
    entrypoint: /year_filter
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./year_filter/middleware_config.yaml
        target: /middleware_config.yaml
''' 

trip_counter_string = ""
for i in range(0, trip_counter_instances):
    trip_counter_string = trip_counter_string + f'''
  trip_counter_{i}:
    container_name: trip_counter_{i}
    environment:
      - ID={i}
      - YEAR_1={year_1}
      - YEAR_2={year_2}
      - PREV_STAGE_INSTANCES={year_filter_instances}
      - NEXT_STAGE_INSTANCES=1
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: trip_counter:latest
    entrypoint: /trip_counter
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./trip_counter/middleware_config.yaml
        target: /middleware_config.yaml
'''   


file_content = f'''services:
  rabbitmq:
    container_name: rabbitmq
    image: rabbitmq:latest
    ports:
      - 5672:5672
      - 15672:15672
    healthcheck:
      test: [ "CMD", "curl", "-f", "http://localhost:15672" ]
      interval: 10s
      timeout: 5s
      retries: 10

  client_handler:
    container_name: client_handler
    environment:
      - MERGER_INSTANCES={merger_instances}
      - DATA_DROPPER_INSTANCES={data_dropper_instances}
      - WEATHER_JOINER_INSTANCES={weather_joiner_instances}
      - STATIONS_JOINER_INSTANCES={stations_joiner_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: client_handler:latest
    entrypoint: /client_handler
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./client_handler/middleware_config.yaml
        target: /middleware_config.yaml

  {client_string}

  duration_merger:
    container_name: duration_merger
    environment:
      - PREV_STAGE_INSTANCES={duration_averager_instances}
      - NEXT_STAGE_INSTANCES={client_handler_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: duration_merger:latest
    entrypoint: /duration_merger
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./duration_merger/middleware_config.yaml
        target: /middleware_config.yaml
  
  count_merger:
    container_name: count_merger
    environment:
      - PREV_STAGE_INSTANCES={trip_counter_instances}
      - NEXT_STAGE_INSTANCES={client_handler_instances}
      - YEAR_1={year_1}
      - YEAR_2={year_2}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: count_merger:latest
    entrypoint: /count_merger
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./count_merger/middleware_config.yaml
        target: /middleware_config.yaml

  distance_merger:
    container_name: distance_merger
    environment:
      - PREV_STAGE_INSTANCES={distance_averager_instances}
      - NEXT_STAGE_INSTANCES={client_handler_instances}
      - MIN_DISTANCE={minimum_distance}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: distance_merger:latest
    entrypoint: /distance_merger
    restart: on-failure
    depends_on:
      rabbitmq:
        condition: service_healthy
    volumes:
      - type: bind
        source: ./distance_merger/middleware_config.yaml
        target: /middleware_config.yaml

{duration_averager_string}        
{precipitation_filter_string}   
{weather_joiner_string}   
{data_dropper_string} 
{stations_joiner_string}
{year_filter_string}
{trip_counter_string}
{distance_calculator_string}
{distance_averager_string}
'''

f = open("compose.yaml", "w")
f.write(file_content)
f.close()