#!/bin/bash

# Configuration
# Hardcoded in filter to avoid quoting hell

echo "Starting git filter-branch rewrite..."

git filter-branch --force --env-filter '
    # Logic to match Zou or Borun
    if [ "$GIT_COMMITTER_EMAIL" = "zou@ZOUdeMacBook-Pro-2.local" ] || [[ "$GIT_COMMITTER_NAME" == *"Borun"* ]] || [[ "$GIT_COMMITTER_NAME" == *"zou"* ]]; then
        export GIT_COMMITTER_NAME="A2Sumie"
        export GIT_COMMITTER_EMAIL="A2Sumie@users.noreply.github.com"
    fi
    if [ "$GIT_AUTHOR_EMAIL" = "zou@ZOUdeMacBook-Pro-2.local" ] || [[ "$GIT_AUTHOR_NAME" == *"Borun"* ]] || [[ "$GIT_AUTHOR_NAME" == *"zou"* ]]; then
        export GIT_AUTHOR_NAME="A2Sumie"
        export GIT_AUTHOR_EMAIL="A2Sumie@users.noreply.github.com"
    fi
' --prune-empty --tag-name-filter cat -- --all

# Note: No files to remove in idol-bbq-utils specified, so removed index-filter
echo "Rewrite complete."
