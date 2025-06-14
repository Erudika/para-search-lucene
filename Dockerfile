FROM eclipse-temurin:21-alpine

RUN mkdir -p /para/lib

WORKDIR /para

ENV PARA_PLUGIN_ID="para-search-lucene" \
	PARA_PLUGIN_VER="1.49.3"

ADD https://repo1.maven.org/maven2/com/erudika/$PARA_PLUGIN_ID/$PARA_PLUGIN_VER/$PARA_PLUGIN_ID-$PARA_PLUGIN_VER-shaded.jar /para/lib/
