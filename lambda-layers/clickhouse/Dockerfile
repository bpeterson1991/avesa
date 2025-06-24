FROM public.ecr.aws/lambda/python:3.10

# Install zip utility
RUN yum update -y && yum install -y zip

# Install dependencies
COPY requirements.txt .
RUN pip install --target /opt/python -r requirements.txt

# Create the layer zip
WORKDIR /opt
RUN zip -r /tmp/clickhouse-layer.zip python/

# Copy the zip file to output
CMD ["cp", "/tmp/clickhouse-layer.zip", "/output/"]
