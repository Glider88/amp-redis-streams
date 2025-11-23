FROM php:8.2-cli-alpine

WORKDIR /usr/src/app

COPY --from=composer:latest /usr/bin/composer /usr/local/bin/composer
ADD --chmod=0755 https://github.com/mlocati/docker-php-extension-installer/releases/latest/download/install-php-extensions /usr/local/bin/

RUN install-php-extensions xdebug pcntl

ENV USER=docker
ENV GROUPNAME=$USER
ENV UID=1000
ENV GID=1000

RUN addgroup --gid "$GID" "$GROUPNAME" \
 && adduser --disabled-password --gecos "" --no-create-home --ingroup "$GROUPNAME" --uid "$UID" $USER

RUN mv "$PHP_INI_DIR/php.ini-development" "$PHP_INI_DIR/php.ini"

ENTRYPOINT ["./.docker/php/entrypoint.sh"]
