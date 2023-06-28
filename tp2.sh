
BuildTp(){

  if [ "$1" ]; then
      docker build -f ./$1/Dockerfile -t "$1:latest" .

  else
      # special
      docker build -f ./client/Dockerfile -t "client:latest" .
      docker build -f ./client_handler/Dockerfile -t "client_handler:latest" .
      docker build -f ./rabbitmq/Dockerfile -t "rabbitmq:latest" .

      # Mergers
      docker build -f ./duration_merger/Dockerfile -t "duration_merger:latest" .
      docker build -f ./distance_merger/Dockerfile -t "distance_merger:latest" .
      docker build -f ./count_merger/Dockerfile -t "count_merger:latest" .

      # Joiners
      docker build -f ./weather_joiner/Dockerfile -t "weather_joiner:latest" .
      docker build -f ./stations_joiner/Dockerfile -t "stations_joiner:latest" .

      # Filters
      docker build -f ./data_dropper/Dockerfile -t "data_dropper:latest" .
      docker build -f ./precipitation_filter/Dockerfile -t "precipitation_filter:latest" .
      docker build -f ./distance_calculator/Dockerfile -t "distance_calculator:latest" .
      docker build -f ./year_filter/Dockerfile -t "year_filter:latest" .

      # Workers
      docker build -f ./duration_averager/Dockerfile -t "duration_averager:latest" .
      docker build -f ./distance_averager/Dockerfile -t "distance_averager:latest" .
      docker build -f ./trip_counter/Dockerfile -t "trip_counter:latest" .

      # Chcker
      docker build -f ./health_checker/Dockerfile -t "health_checker:latest" .

  fi
}

StopTp(){
    
    docker compose -f compose.yaml stop -t 5

    if [ "$1" == "-k" ]; then
    docker compose -f compose.yaml down -t 1 --remove-orphans
    fi

}   

RunTp(){

    if [ "$1" == "logs" ]; then

        # Show logs
        docker compose -f compose.yaml logs -f

    elif [ "$1" == "monkey" ]; then

      # Temporary container to test the failure prevention
      docker run --rm --mount type=bind,src="$(PWD)/crazy_monkey/",dst="/app/"\
                      --mount type=bind,src="/var/run/docker.sock",dst="/var/run/docker.sock"\
                      -i -t --entrypoint python3 "health_checker:latest" main.py

    else

        # Docker Compose Up
        rm -rf ./data/recovery_data
        python3 create_docker_compose.py
        docker compose up --build

    fi

}

DiffTp(){
    if [ "$1" == "medium" ]; then
        diff -u -w --color correct_results/results_medium_1.txt data/recovery_data/client_0/result_1.txt
        diff -u -w --color correct_results/results_medium_2.txt data/recovery_data/client_0/result_2.txt
        diff -u -w --color correct_results/results_medium_3.txt data/recovery_data/client_0/result_3.txt
    elif [ "$1" == "large" ]; then
        diff -u -w --color correct_results/results_large_1.txt data/recovery_data/client_0/result_1.txt
        diff -u -w --color correct_results/results_large_2.txt data/recovery_data/client_0/result_2.txt
        diff -u -w --color correct_results/results_large_3.txt data/recovery_data/client_0/result_3.txt
    else
        diff -u -w --color correct_results/results_full_1.txt data/recovery_data/client_0/result_1.txt
        diff -u -w --color correct_results/results_full_2.txt data/recovery_data/client_0/result_2.txt
        diff -u -w --color correct_results/results_full_3.txt data/recovery_data/client_0/result_3.txt
    fi

    for (( i=1 ; i<$2 ; i++ ))
    do
        diff -u -w --color "data/recovery_data/client_$i/result_1.txt" data/recovery_data/client_0/result_1.txt
        diff -u -w --color "data/recovery_data/client_$i/result_2.txt" data/recovery_data/client_0/result_2.txt
        diff -u -w --color "data/recovery_data/client_$i/result_3.txt" data/recovery_data/client_0/result_3.txt
    done
}

myfun(){
    echo $1 $2 $3 $4
}

if [ "$1" == "build" ]; then
    shift
    BuildTp $@

elif [ "$1" == "run" ]; then
    shift
    RunTp $@

elif [ "$1" == "stop" ]; then
    shift
    StopTp $@

elif [ "$1" == "diff" ]; then
    shift
    DiffTp $@

elif [ "$1" == "help" ]; then

    echo ""
    echo "  - build [image]      Buildea todas las imagenes o la imagen especificada."
    echo ""
    echo "  - run [logs]         Inicia la arquitectura definida en 'composer.py'"
    echo "                           test: Instancia de netcat conectada a la misma red"
    echo "                           logs: Muestra los logs"
    echo ""
    echo "  - stop [-k]          Detiene los contenedores corriendo"
    echo "                           -k: elimina los contenedores"
    echo ""

else
    echo "Comando desconocido.."
    echo "Prueba con $0 help"
fi


