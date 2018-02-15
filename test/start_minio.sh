set -e

CONTAINER_ID_FILE="minio_container_id"

docker run -d \
	--rm \
	-p 9042:9000 \
	-e "MINIO_ACCESS_KEY=access_key" \
	-e "MINIO_SECRET_KEY=secret_key" \
	-v $(pwd)/test:/export \
	minio/minio \
	server /export \
> $CONTAINER_ID_FILE

sleep 1
