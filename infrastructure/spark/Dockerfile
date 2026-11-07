FROM spark:4.0.1-python3@sha256:a552a335e0aedb44fa28b20ed7e9dc52c2e856faf87fb0409fb57dbf37b45bf7
USER root
COPY requirements.lock /tmp/requirements.lock
RUN pip install --no-cache-dir -r /tmp/requirements.lock
ENV PYTHONPATH=/workspace:/workspace/backend:/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.9-src.zip
RUN mkdir -p /data /opt/spark/.ivy2 && chown -R 185:0 /data /opt/spark/.ivy2
WORKDIR /workspace
USER 185
