# build the image:
```
docker build -t vmhub-pipeline:local .
```

# run with environment variables:
```
docker run \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/credentials/credentials.json:/app/credentials/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/credentials.json \
  -e PYTHONPATH=/app \
  vmhub-pipeline:local
```


```
docker build -t vmhub-pipeline:local .

docker run \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/credentials/credentials.json:/app/credentials/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/credentials.json \
  -e PYTHONPATH=/app \
  vmhub-pipeline:local
```