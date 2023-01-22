#!/bin/bash

echo "Project name: "
read project_name

echo "Decompressing template.zip"
unzip template.zip && mv template $project_name

for file in $project_name/*; do
    if [ -f $file ]; then
        echo $file
        sed -Ei "s|%PROJECT_NAME%|$project_name|g" $file
    fi
done