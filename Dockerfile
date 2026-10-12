FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY tracker_backup.py .

RUN useradd --uid 10001 --create-home tracker && mkdir /backup && chown tracker /backup
USER tracker
VOLUME /backup

ENTRYPOINT ["python", "/app/tracker_backup.py"]
CMD ["--out", "/backup"]
