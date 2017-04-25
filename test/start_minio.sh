set -e

CONTAINER_ID_FILE="minio_container_id"

if [ -f $CONTAINER_ID_FILE ]
then
	docker rm -vf $(cat minio_container_id)
	rm minio_container_id
fi

docker run -d \
	--name file-disk-minio \
	-p 9042:9000 \
	-e "MINIO_ACCESS_KEY=access_key" \
	-e "MINIO_SECRET_KEY=secret_key" \
	-v $(pwd)/test/fixtures:/export/bucket \
	minio/minio \
	server /export \
> $CONTAINER_ID_FILE

sleep 1
