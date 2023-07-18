FROM python:3.10-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Copy the entire cloud-run directory to the container image.
ENV APP_HOME /app
WORKDIR $APP_HOME
COPY cloud-run/ $APP_HOME/cloud-run/

# Install production dependencies.
RUN pip install --no-cache-dir -r $APP_HOME/cloud-run/requirements.txt

# Execute
CMD exec python $APP_HOME/cloud-run/app.py
