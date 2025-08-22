FROM eclipse-temurin:17-jre-jammy

WORKDIR /otp

COPY otp-1.5.0-shaded.jar /otp/otp-1.5.0-shaded.jar
RUN python -m pip install --no-cache-dir awscli

ENV JAVA_OPTS="-Xmx8G"
# env provided at runtime:
#  AWS_REGION, GRAPHS_BUCKET, OSM_PREFIX, SCENARIO_ID, PREFECTURE, S3_GTFS_URI

CMD ["/bin/sh","-lc","set -e; \
  test -n \"$GRAPHS_BUCKET\" || (echo 'GRAPHS_BUCKET not set' && exit 1); \
  test -n \"$OSM_PREFIX\" || (echo 'OSM_PREFIX not set' && exit 1); \
  test -n \"$SCENARIO_ID\" || (echo 'SCENARIO_ID not set' && exit 1); \
  test -n \"$PREFECTURE\" || (echo 'PREFECTURE not set' && exit 1); \
  test -n \"$S3_GTFS_URI\" || (echo 'S3_GTFS_URI not set' && exit 1); \
  mkdir -p /work; \
  echo \"Downloading OSM s3://$GRAPHS_BUCKET/$OSM_PREFIX/$PREFECTURE.pbf\"; \
  aws s3 cp s3://$GRAPHS_BUCKET/$OSM_PREFIX/$PREFECTURE.pbf /work/$PREFECTURE.pbf --region $AWS_REGION; \
  echo \"Downloading GTFS $S3_GTFS_URI\"; \
  aws s3 cp $S3_GTFS_URI /work/gtfs.zip --region $AWS_REGION; \
  echo \"Building graph...\"; \
  exec java $JAVA_OPTS -jar /otp/otp-1.5.0-shaded.jar --build /work; \
  echo \"Uploading Graph.obj\"; \
  aws s3 cp /work/Graph.obj s3://$GRAPHS_BUCKET/graphs/$SCENARIO_ID/Graph.obj --region $AWS_REGION" ]
