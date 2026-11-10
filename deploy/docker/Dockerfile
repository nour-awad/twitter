# One parametrised image recipe for every service (build context = repo root).
#   docker build -f deploy/docker/Dockerfile \
#     --build-arg SERVICE_DIR=services/tweet-service --build-arg APP_MODULE=tweet_service.main:app .
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

# Shared chassis first => cached layer reused by every service image.
COPY libs/common /app/libs/common
RUN pip install /app/libs/common

ARG SERVICE_DIR
ARG APP_MODULE
COPY ${SERVICE_DIR} /app/service
RUN if [ -f /app/service/requirements.txt ]; then pip install -r /app/service/requirements.txt; fi

WORKDIR /app/service
# Run as an unprivileged user (zero trust starts inside the container).
RUN useradd --uid 10001 --no-create-home app && mkdir -p /app/data && chown -R app /app/data
USER 10001

ENV APP_MODULE=${APP_MODULE} \
    PORT=8000 \
    DATA_DIR=/app/data

EXPOSE 8000
HEALTHCHECK --interval=10s --timeout=3s --start-period=20s --retries=3 \
  CMD python -c "import os,urllib.request; urllib.request.urlopen('http://127.0.0.1:%s/health/live' % os.environ['PORT'])"

CMD ["sh", "-c", "exec uvicorn ${APP_MODULE} --host 0.0.0.0 --port ${PORT} --proxy-headers --no-access-log"]
