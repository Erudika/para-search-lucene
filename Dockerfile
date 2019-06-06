FROM openjdk:8-jdk-alpine

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-search-lucene" \
	PARA_PLUGIN_VER="1.31.4"

RUN wget -q -P /para/lib/ https://oss.sonatype.org/service/local/repositories/releases/content/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar \
	|| exit 2
