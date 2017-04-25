Run on the command line or build the container and run it
like this:

<pre>
docker run -e DB_USER=$DB_USER --link mypg:postgres -e DB_PASSWORD=$DB_PASSWORD \
-e DB_PORT=$DB_PORT -e DB_NAME=$DB_NAME -e DB_HOST=postgres \
-e http_proxy=$http_proxy \
-e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY \
-e EVENT_QUEUE_URL=$EVENT_QUEUE_URL \
-e AWS_REGION=$AWS_REGION xtracdev/ecsatomdata
</pre>
