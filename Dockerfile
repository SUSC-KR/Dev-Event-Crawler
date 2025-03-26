FROM quay.io/astronomer/astro-runtime:12.7.1

USER root
RUN apt-get update && apt-get install -y git && rm -rf /var/lib/apt/lists/*

USER astro