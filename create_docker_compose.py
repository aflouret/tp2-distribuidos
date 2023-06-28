import os
from configparser import ConfigParser

config = ConfigParser(os.environ)
config.read("config.ini")

rabbitmq_connection_string = config["DEFAULT"]["RABBITMQ_CONNECTION_STRING"]
client_handler_address = config["DEFAULT"]["CLIENT_HANDLER_ADDRESS"]

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

health_checker_instances = int(config["DEFAULT"]["HEALTH_CHECKER_INSTANCES"])
health_checker_targets = []

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
      - ./data/recovery_data/client_{i}:/recovery_files/
'''


trip_counter_string = ""
trip_counter_dependency_string = ""
for i in range(0, trip_counter_instances):
    health_checker_targets.append(f"trip_counter_{i}")
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
      - count_merger
    volumes:
      - type: bind
        source: ./trip_counter/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/trip_counter_{i}:/recovery_files/
'''
    trip_counter_dependency_string = trip_counter_dependency_string + f'''      - trip_counter_{i}
'''


duration_averager_string = ""
duration_averager_dependency_string = ""
for i in range(0, duration_averager_instances):
    health_checker_targets.append(f"duration_averager_{i}")
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
      - duration_merger
    volumes:
      - type: bind
        source: ./duration_averager/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/duration_averager_{i}:/recovery_files/
'''
    duration_averager_dependency_string = duration_averager_dependency_string + f'''      - duration_averager_{i}
'''


distance_averager_string = ""
distance_averager_dependency_string = ""
for i in range(0, distance_averager_instances):
    health_checker_targets.append(f"distance_averager_{i}")
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
      - distance_merger
    volumes:
      - type: bind
        source: ./distance_averager/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/distance_averager_{i}:/recovery_files/
'''
    distance_averager_dependency_string = distance_averager_dependency_string + f'''      - distance_averager_{i}
'''


precipitation_filter_string = ""
precipitation_filter_dependency_string = ""
for i in range(0, precipitation_filter_instances):
    health_checker_targets.append(f"precipitation_filter_{i}")
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
{duration_averager_dependency_string}
    volumes:
      - type: bind
        source: ./precipitation_filter/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/precipitation_filter_{i}:/recovery_files/
'''
    precipitation_filter_dependency_string = precipitation_filter_dependency_string + f'''      - precipitation_filter_{i}
'''

distance_calculator_string = ""
distance_calculator_dependency_string = ""
for i in range(0, distance_calculator_instances):
    health_checker_targets.append(f"distance_calculator_{i}")
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
{distance_averager_dependency_string}
    volumes:
      - type: bind
        source: ./distance_calculator/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/distance_calculator_{i}:/recovery_files/
'''
    distance_calculator_dependency_string = distance_calculator_dependency_string + f'''      - distance_calculator_{i}
'''


year_filter_string = ""
year_filter_dependency_string = ""
for i in range(0, year_filter_instances):
    health_checker_targets.append(f"year_filter_{i}")
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
{trip_counter_dependency_string}
    volumes:
      - type: bind
        source: ./year_filter/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/year_filter_{i}:/recovery_files/
'''
    year_filter_dependency_string = year_filter_dependency_string + f'''      - year_filter_{i}
'''

weather_joiner_string = ""
weather_joiner_dependency_string = ""
for i in range(0, weather_joiner_instances):
    health_checker_targets.append(f"weather_joiner_{i}")
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
{precipitation_filter_dependency_string}
    volumes:
      - type: bind
        source: ./weather_joiner/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/weather_joiner_{i}:/recovery_files/
''' 
    weather_joiner_dependency_string = weather_joiner_dependency_string + f'''      - weather_joiner_{i}
'''

stations_joiner_string = ""
stations_joiner_dependency_string = ""
for i in range(0, stations_joiner_instances):
    health_checker_targets.append(f"stations_joiner_{i}")
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
{year_filter_dependency_string}
{distance_calculator_dependency_string}
    volumes:
      - type: bind
        source: ./stations_joiner/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/stations_joiner_{i}:/recovery_files/
'''
    stations_joiner_dependency_string = stations_joiner_dependency_string + f'''      - stations_joiner_{i}
'''


data_dropper_string = ""
data_dropper_dependency_string = ""
for i in range(0, data_dropper_instances):
    health_checker_targets.append(f"data_dropper_{i}")
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
{weather_joiner_dependency_string}
{stations_joiner_dependency_string}
    volumes:
      - type: bind
        source: ./data_dropper/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/data_dropper_{i}:/recovery_files/
'''

    data_dropper_dependency_string = data_dropper_dependency_string + f'''      - data_dropper_{i}
'''

non_scaled_nodes= ["client_handler","duration_merger","count_merger","distance_merger"]
health_checker_string = ""
health_checker_peers = ",".join([ "health_checker_" + str(i) for i in range(health_checker_instances) ])
health_checker_targets.extend(health_checker_peers.split(","))
health_checker_targets.extend(non_scaled_nodes)
health_checker_targets = ",".join(health_checker_targets)

for i in range(0, health_checker_instances):
    health_checker_string = health_checker_string + f'''
  health_checker_{i}:
    container_name: health_checker_{i}
    environment:
      - HOSTNAME=health_checker_{i}
      - PEERS={health_checker_peers}
      - TARGETS={health_checker_targets}
    image: health_checker:latest
    entrypoint: python3 /app/main.py
    restart: on-failure
    volumes:
        - ./health_checker/main.py:/app/main.py
        - ./health_checker/health_checker.py:/app/health_checker.py
        - ./health_checker/messaging_protocol.py:/app/messaging_protocol.py
        - ./health_checker/leader_election/:/app/leader_election/
        - ./health_checker/replier/replier.py:/app/replier.py
        - /var/run/docker.sock:/var/run/docker.sock
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
      - ID=client_handler
      - ADDRESS={client_handler_address}
      - MERGER_INSTANCES={merger_instances}
      - DATA_DROPPER_INSTANCES={data_dropper_instances}
      - WEATHER_JOINER_INSTANCES={weather_joiner_instances}
      - STATIONS_JOINER_INSTANCES={stations_joiner_instances}
      - RABBITMQ_CONNECTION_STRING={rabbitmq_connection_string}
    image: client_handler:latest
    entrypoint: /client_handler
    restart: on-failure
    depends_on:
{data_dropper_dependency_string}
    volumes:
      - type: bind
        source: ./client_handler/middleware_config.yaml
        target: /middleware_config.yaml
      - ./data/recovery_data/client_handler:/recovery_files/

  {client_string}

  duration_merger:
    container_name: duration_merger
    environment:
      - ID=duration_merger
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
      - ./data/recovery_data/duration_merger:/recovery_files/
  
  count_merger:
    container_name: count_merger
    environment:
      - ID=count_merger
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
      - ./data/recovery_data/count_merger:/recovery_files/

  distance_merger:
    container_name: distance_merger
    environment:
      - ID=distance_merger
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
      - ./data/recovery_data/distance_merger:/recovery_files/

{duration_averager_string}        
{precipitation_filter_string}   
{weather_joiner_string}   
{data_dropper_string} 
{stations_joiner_string}
{year_filter_string}
{trip_counter_string}
{distance_calculator_string}
{distance_averager_string}
{health_checker_string}
'''

f = open("compose.yaml", "w")
f.write(file_content)
f.close()