FROM python:3.9.9-bullseye

ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_VERSION=1.1.0 \
    POETRY_VIRTUALENVS_CREATE=0 

# System deps:
RUN pip install "poetry==$POETRY_VERSION"

RUN mkdir -p /app
WORKDIR /app

COPY pyproject.toml poetry.lock ./
RUN poetry install --no-dev --no-root

COPY main.py ./
CMD ["python", "main.py"]
