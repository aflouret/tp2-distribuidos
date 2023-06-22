
if [ "$1"  == "build" ]; then
    docker build -f ../DockerfilePython ../ -t checker-test:latest
    docker build -f ../replier/DockerfileGo ../ -t replier-test:latest

    shift
fi

if [ "$1" == "complete" ]; then
  docker compose -f health-checker-complete-test.yaml up
else
  docker compose -f checker-test-compose.yaml up
fi
