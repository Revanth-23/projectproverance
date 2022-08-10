FROM 172.16.0.154:5060/python-openjdk-microservice:39-8-312-ol8

COPY requirements.txt /

RUN pip install --no-cache-dir -r /requirements.txt \
    && rm -rf /root/.cache

ENV APP_HOME=/usr/local/application

COPY ./ ${APP_HOME}/

# RUN chmod +x ${APP_HOME}/bin/Raijin.py ${APP_HOME}/health.sh
RUN chmod +x ${APP_HOME}/entrypoint.sh ${APP_HOME}/health.sh

WORKDIR /

# HEALTHCHECK CMD ${APP_HOME}/health.sh


# ENTRYPOINT ["python", "/usr/local/application/bin/Raijin.py"]
ENTRYPOINT ["bash", "/usr/local/application/entrypoint.sh"]
