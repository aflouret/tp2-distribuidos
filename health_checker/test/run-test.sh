
if [ "$1"  == "build" ]; then
    docker build ../ -t checker-test:latest
fi

docker compose -f checker-test-compose.yaml up