#!/bin/sh

find . ! -name 'setup.sh' -exec rm -rf {} +

# Update below according to https://jena.apache.org/download/
# and checksum for apache-jena-3.x.x.tar.gz.sha512
JENA_SHA512="db8d05c28d890b5456f08e277d04c6b15fa10c20740430ce60de38689ff136e21a3b1cce298cd429c21b7af0a0a858080880b3fee0e121d9b68801e0b67ec51b"
JENA_VERSION="4.7.0"
# No need for https due to sha512 checksums below
ASF_MIRROR="http://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename="
ASF_ARCHIVE="http://archive.apache.org/dist/"

echo "$JENA_SHA512  jena.tar.gz" > jena.tar.gz.sha512
(
    curl --location --silent --show-error --fail --retry-connrefused --retry 3 --output jena.tar.gz ${ASF_MIRROR}jena/binaries/apache-jena-$JENA_VERSION.tar.gz || \
    curl --fail --silent --show-error --retry-connrefused --retry 3 --output jena.tar.gz $ASF_ARCHIVE/jena/binaries/apache-jena-$JENA_VERSION.tar.gz
) && \
sha512sum -c jena.tar.gz.sha512 && \
tar zxf jena.tar.gz && \
mv apache-jena* ./jena && \
rm jena.tar.gz* && \
cd ./jena && rm -rf *javadoc* *src* bat