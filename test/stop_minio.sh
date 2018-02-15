set -e

CONTAINER_ID_FILE="minio_container_id"

if [ -f $CONTAINER_ID_FILE ]
then
	docker kill $(cat $CONTAINER_ID_FILE)
	rm $CONTAINER_ID_FILE
fi
