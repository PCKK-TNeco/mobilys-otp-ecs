FROM eclipse-temurin:11-jre
WORKDIR /otp

# --- Provide the OTP shaded jar in the build context ---
# IMPORTANT: otp-1.5.0-shaded.jar must be in the otp/ folder next to this Dockerfile.
COPY otp-1.5.0-shaded.jar /otp/otp-1.5.0-shaded.jar

# --- Install AWS CLI v2 (official installer) ---
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl unzip \
 && curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "/tmp/awscliv2.zip" \
 && unzip /tmp/awscliv2.zip -d /tmp \
 && /tmp/aws/install \
 && rm -rf /var/lib/apt/lists/* /tmp/aws /tmp/awscliv2.zip

# JVM heap for heavy graph build (adjust if needed)
ENV JAVA_OPTS="-Xmx8g"

# ECS runtime env (set at task runtime)
ENV AWS_REGION=""
ENV GRAPHS_BUCKET=""
ENV OSM_PREFIX=""
ENV OSM_EXT=""  
ENV SCENARIO_ID=""
ENV PREFECTURE=""
ENV S3_GTFS_URI=""

CMD ["/bin/sh","-lc","\
  set -euo; \
  : ${AWS_REGION:?AWS_REGION not set}; \
  : ${GRAPHS_BUCKET:?GRAPHS_BUCKET not set}; \
  : ${OSM_PREFIX:?OSM_PREFIX not set}; \
  : ${SCENARIO_ID:?SCENARIO_ID not set}; \
  : ${PREFECTURE:?PREFECTURE not set}; \
  : ${S3_GTFS_URI:?S3_GTFS_URI not set}; \
  OSM_EXT_VAL=\"${OSM_EXT:-.osm.pbf}\"; \
  OSM_KEY=\"${OSM_PREFIX%/}/${PREFECTURE}${OSM_EXT_VAL}\"; \
  \
  mkdir -p /work; \
  echo \"Checking OSM s3://${GRAPHS_BUCKET}/${OSM_KEY}\"; \
  if ! aws s3 ls \"s3://${GRAPHS_BUCKET}/${OSM_KEY}\" --region \"${AWS_REGION}\" >/dev/null 2>&1; then \
    echo \"ERROR: OSM not found. Expected s3://${GRAPHS_BUCKET}/${OSM_KEY}\" >&2; \
    echo \"       (Set OSM_PREFIX and OSM_EXT correctly, and PREFECTURE must match the filename stem)\" >&2; \
    exit 1; \
  fi; \
  echo \"Downloading OSM s3://${GRAPHS_BUCKET}/${OSM_KEY}\"; \
  aws s3 cp \"s3://${GRAPHS_BUCKET}/${OSM_KEY}\" \"/work/$(basename \"${OSM_KEY}\")\" --region \"${AWS_REGION}\"; \
  \
  echo \"Downloading GTFS ${S3_GTFS_URI}\"; \
  aws s3 cp \"${S3_GTFS_URI}\" /work/gtfs.zip --region \"${AWS_REGION}\"; \
  \
  echo \"Building graph...\"; \
  java ${JAVA_OPTS} -jar /otp/otp-1.5.0-shaded.jar --build /work; \
  \
  echo \"Uploading Graph.obj\"; \
  aws s3 cp /work/Graph.obj \"s3://${GRAPHS_BUCKET}/graphs/${SCENARIO_ID}/Graph.obj\" --region \"${AWS_REGION}\" \
"]
