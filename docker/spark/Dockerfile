FROM apache/spark:4.0.1-scala2.13-java17-python3-ubuntu@sha256:acfe7e06e95dd13aa32ee03c464766900d52fba15f58e421f8e1213ec041bb94

USER root
RUN python3 -m pip install --no-cache-dir \
	duckdb==1.4.1 \
	pydantic-settings==2.10.1 \
	python-dotenv==1.1.1

WORKDIR /opt/streamlake