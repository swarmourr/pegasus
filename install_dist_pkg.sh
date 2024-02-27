#!/bin/bash

# Navigate to the directory containing the distribution files (dist/)
cd dist
# Uninstall pegasus
pip list | awk '$1 ~ /^pegasus-wms/ {print $1}' | xargs -n 1 pip uninstall -y

# Install the specific version
pip install pegasus-wms-5.0.7.dev0.tar.gz

# Install any version matching the pattern
pip install pegasus-wms-*.tar.gz

# Alternatively, you can use pip3 for Python 3
pip3 install pegasus-wms-*.tar.gz

# Install the specific package if needed
pip3 install pegasus-wms.api-5.0.7.dev0.tar.gz
