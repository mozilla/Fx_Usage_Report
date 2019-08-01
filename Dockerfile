FROM openjdk:8

# add a non-privileged user for running the application
RUN groupadd --gid 10001 app && \
    useradd -g app --uid 10001 --shell /usr/sbin/nologin --create-home --home-dir /app app

WORKDIR /app

# Install python
RUN apt-get update && \
    apt-get -y --no-install-recommends install python2.7 python-pip python-setuptools

# Finally copy in the app's source file
COPY . /app

ENV PYTHONPATH $PYTHONPATH:/app/usage_report:/app/tests

RUN pip install -r requirements.txt

USER app
