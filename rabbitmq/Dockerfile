FROM rabbitmq:3.12-management-alpine
RUN apk update && apk add curl
RUN mkdir -p /etc/rabbitmq/conf.d
RUN chown -R rabbitmq:rabbitmq /etc/rabbitmq/conf.d
COPY rabbitmq/20-logging.conf /etc/rabbitmq/conf.d