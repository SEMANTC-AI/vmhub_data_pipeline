```
docker build -t vmhub-pipeline:local .

docker run \
  -v $(pwd)/.env:/app/.env \
  -v $(pwd)/credentials/credentials.json:/app/credentials/credentials.json:ro \
  -e GOOGLE_APPLICATION_CREDENTIALS=/app/credentials/credentials.json \
  -e PYTHONPATH=/app \
  -e USER_ID=UYL7W9kaSsYFIFMPzbJoYoHADb33 \
  vmhub-pipeline:local
```




<!-- RUN THE BUILD: -->
```
chmod +x scripts/deploy.sh
./scripts/deploy.sh
```