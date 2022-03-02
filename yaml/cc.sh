#!/usr/bin/env bash
echo "create template file"
cat start.yml functions.yml api.yml iam.yml invoce.yml layers.yml sheduler.yml sns.yml table.yml > ../template.yaml
