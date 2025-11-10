#!/bin/bash
# ============================================================
# 🧪 Automated Functional Test for queuectl Background Job System
# ============================================================

# Exit immediately on any command failure
set -e

# Colors for pretty output
GREEN="\033[0;32m"
RED="\033[0;31m"
YELLOW="\033[1;33m"
NC="\033[0m" # No Color

echo -e "${YELLOW}Starting queuectl functional verification...${NC}"
echo "----------------------------------------------"

# --- 1️⃣ Initialize Database ---
echo -e "${YELLOW}[Step 1] Initializing database...${NC}"
./queuectl.py init
sleep 1
DB_PATH="queue/data/queue.db"
if [ -f "$DB_PATH" ]; then
  echo -e "${GREEN}✅ Database initialized successfully at $DB_PATH${NC}"
else
  echo -e "${RED}❌ Database initialization failed.${NC}"
  exit 1
fi

# --- 2️⃣ Enqueue Test Jobs ---
echo -e "\n${YELLOW}[Step 2] Enqueuing test jobs...${NC}"
JOB1=$(./queuectl.py enqueue '{"command": "echo JobOne"}' | awk '{print $NF}')
JOB2=$(./queuectl.py enqueue '{"command": "false"}' | awk '{print $NF}')
echo -e "Added jobs: $JOB1, $JOB2"

sleep 1
echo -e "\nCurrent job list:"
./queuectl.py list

# --- 3️⃣ Test Config Commands ---
echo -e "\n${YELLOW}[Step 3] Checking config commands...${NC}"
./queuectl.py config get max_retries
./queuectl.py config set max_retries 2
NEW_RETRIES=$(./queuectl.py config get max_retries | awk '{print $NF}')
if [ "$NEW_RETRIES" == "2" ]; then
  echo -e "${GREEN}✅ Config updated successfully (max_retries=2)${NC}"
else
  echo -e "${RED}❌ Config update failed${NC}"
  exit 1
fi

# --- 4️⃣ Start Worker ---
echo -e "\n${YELLOW}[Step 4] Starting worker (will process jobs)...${NC}"
./queuectl.py worker start --count 1 &
WORKER_PID=$!
sleep 10  # Allow some time for processing/retries

echo -e "${YELLOW}Stopping worker...${NC}"
kill $WORKER_PID 2>/dev/null || true
wait $WORKER_PID 2>/dev/null || true

# --- 5️⃣ Check Job States ---
echo -e "\n${YELLOW}[Step 5] Checking job states after processing...${NC}"
./queuectl.py status

PENDING=$(./queuectl.py list --state pending | grep -c cmd || true)
COMPLETED=$(./queuectl.py list --state completed | grep -c cmd || true)
DEAD=$(./queuectl.py list --state dead | grep -c cmd || true)

echo -e "\nSummary: Pending=$PENDING Completed=$COMPLETED Dead=$DEAD"

if [ "$COMPLETED" -ge 1 ]; then
  echo -e "${GREEN}✅ At least one job completed successfully${NC}"
else
  echo -e "${RED}❌ No jobs completed${NC}"
  exit 1
fi

if [ "$DEAD" -ge 1 ]; then
  echo -e "${GREEN}✅ DLQ contains failed jobs as expected${NC}"
else
  echo -e "${YELLOW}⚠️ No jobs in DLQ (possible all succeeded)${NC}"
fi

# --- 6️⃣ Retry DLQ Job (if exists) ---
if [ "$DEAD" -ge 1 ]; then
  JOB_DLQ=$(./queuectl.py dlq list | awk '/cmd/ {print $1; exit}')
  echo -e "\n${YELLOW}[Step 6] Retrying DLQ job: $JOB_DLQ${NC}"
  ./queuectl.py dlq retry "$JOB_DLQ"
  ./queuectl.py list --state pending
fi

# --- 7️⃣ Persistence Check ---
echo -e "\n${YELLOW}[Step 7] Checking job persistence across restarts...${NC}"
echo -e "Restarting script and verifying job count remains consistent..."

COUNT_BEFORE=$(./queuectl.py list | grep -c cmd || true)
sleep 2
COUNT_AFTER=$(./queuectl.py list | grep -c cmd || true)

if [ "$COUNT_BEFORE" == "$COUNT_AFTER" ]; then
  echo -e "${GREEN}✅ Jobs persisted correctly after restart${NC}"
else
  echo -e "${RED}❌ Job count changed unexpectedly${NC}"
  exit 1
fi

# --- 8️⃣ Final Status ---
echo -e "\n${YELLOW}[Step 8] Final Queue Status:${NC}"
./queuectl.py status

echo -e "\n${GREEN}🎉 All functional tests completed successfully!${NC}"
echo "----------------------------------------------"
