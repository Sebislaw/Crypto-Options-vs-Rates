#!/bin/bash

# --- CONFIGURATION ---
NIFI_HOME="/usr/local/nifi-1.14.0"
TEMPLATE_PATH="/home/vagrant/Crypto-Options-vs-Rates/ingestion_layer/nifi/NiFi_Flow.xml"
API_URL="http://localhost:9443/nifi-api"

# --- PRE-FLIGHT CHECK: Install jq ---
# This checks if jq exists. If not, it installs it.
if ! command -v jq &> /dev/null; then
    echo "jq command not found. Installing..."
    sudo apt-get update
    sudo apt-get install -y jq
fi

# 1. Wait for NiFi to be healthy
echo "Waiting for NiFi API to accept requests at $API_URL..."
while ! curl -s -o /dev/null "$API_URL/flow/process-groups/root"; do
    printf "."
    sleep 5
done
echo -e "\nNiFi is UP!"

# 2. Upload the Template
echo "--- Uploading Template ---"
# Try upload first. grep gets the ID from the XML response.
TEMPLATE_ID=$(curl -s -F template=@$TEMPLATE_PATH "$API_URL/process-groups/root/templates/upload" | grep -oP '(?<=<id>).*?(?=</id>)' | head -1)

if [ -z "$TEMPLATE_ID" ]; then
    echo "Template might already exist. Attempting to find existing..."
    TEMPLATE_NAME=$(basename "$TEMPLATE_PATH" .xml)
    
    # Use jq to extract the ID of the existing template
    RESPONSE=$(curl -s "$API_URL/flow/templates")
    TEMPLATE_ID=$(echo "$RESPONSE" | jq -r ".templates[] | select(.template.name==\"$TEMPLATE_NAME\") | .id")
fi

if [ -z "$TEMPLATE_ID" ]; then
    echo "ERROR: Could not find or upload template. Exiting."
    exit 1
fi

echo "Template ID: $TEMPLATE_ID"

# 4. Instantiate Flow
echo "--- Instantiating Flow ---"
curl -s -X POST -H "Content-Type: application/json" \
    -d "{\"templateId\":\"$TEMPLATE_ID\",\"originX\":100,\"originY\":100}" \
    "$API_URL/process-groups/root/template-instance" > /dev/null

echo "Flow placed on canvas."

# 5. Enable Services
echo "--- Enabling Controller Services ---"
# Get list of all disabled services in the root group
SERVICES_JSON=$(curl -s "$API_URL/flow/process-groups/root/controller-services")
SERVICES=$(echo "$SERVICES_JSON" | jq -c '.controllerServices[] | select(.status.runStatus=="DISABLED")')

# If SERVICES is empty, skip loop
if [ ! -z "$SERVICES" ]; then
    echo "$SERVICES" | while read -r SERVICE_JSON; do
        if [ ! -z "$SERVICE_JSON" ]; then
            SVC_ID=$(echo $SERVICE_JSON | jq -r '.id')
            REVISION=$(echo $SERVICE_JSON | jq -r '.revision.version')
            echo "Enabling Service: $SVC_ID (Rev: $REVISION)"
            curl -s -X PUT -H "Content-Type: application/json" \
                -d "{\"revision\":{\"version\":$REVISION},\"state\":\"ENABLED\"}" \
                "$API_URL/controller-services/$SVC_ID/run-status" > /dev/null
        fi
    done
else
    echo "No disabled services found (or jq parse failed)."
fi

echo "Waiting for services to start..."
sleep 5

# 6. Start Processors
echo "--- Starting Processors ---"
# Get list of all stopped processors in the root group
PROCESSORS_JSON=$(curl -s "$API_URL/flow/process-groups/root")
PROCESSORS=$(echo "$PROCESSORS_JSON" | jq -c '.processGroupFlow.flow.processors[] | select(.status.runStatus=="Stopped")')

if [ ! -z "$PROCESSORS" ]; then
    echo "$PROCESSORS" | while read -r PROC_JSON; do
        if [ ! -z "$PROC_JSON" ]; then
            PROC_ID=$(echo $PROC_JSON | jq -r '.id')
            REVISION=$(echo $PROC_JSON | jq -r '.revision.version')
            echo "Starting Processor: $PROC_ID (Rev: $REVISION)"
            curl -s -X PUT -H "Content-Type: application/json" \
                -d "{\"revision\":{\"version\":$REVISION},\"state\":\"RUNNING\"}" \
                "$API_URL/processors/$PROC_ID/run-status" > /dev/null
        fi
    done
else
    echo "No stopped processors found."
fi

echo "--- DEPLOYMENT COMPLETE ---"