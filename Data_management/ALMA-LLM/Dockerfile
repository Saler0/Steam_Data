FROM python:3.10-slim
RUN apt-get update && apt-get install -y libsentencepiece-dev && \
    pip install transformers sentencepiece bitsandbytes accelerate
WORKDIR /app
COPY llm_dowload.py .
ENTRYPOINT ["python", "llm_dowload.py"]
