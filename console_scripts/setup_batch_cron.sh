#!/bin/bash

# ==============================================================================
# Script Name: setup_batch_cron.sh
# Description: Setup and manage cron jobs for batch analytics processing
# Usage: ./setup_batch_cron.sh [install|uninstall|status]
# ==============================================================================

PROJECT_ROOT="$HOME/Crypto-Options-vs-Rates"
BATCH_SCRIPT="$PROJECT_ROOT/batch_layer/run_batch_analytics.sh"
LOG_DIR="$PROJECT_ROOT/logs/batch_cron"
CRON_MARKER="# Crypto Batch Analytics Job"
CRON_SCHEDULE="0 */6 * * *"  # Run every 6 hours at minute 0

# Create log directory
mkdir -p "$LOG_DIR"

# Function to show usage
show_usage() {
    echo "Usage: $0 [install|uninstall|status|logs]"
    echo ""
    echo "Commands:"
    echo "  install     - Install cron job for batch analytics"
    echo "  uninstall   - Remove cron job"
    echo "  status      - Show cron job status and last execution"
    echo "  logs        - Show recent batch execution logs"
    exit 1
}

# Function to install cron job
install_cron() {
    echo "============================================================"
    echo "  Installing Batch Analytics Cron Job"
    echo "============================================================"
    
    # Check if batch script exists
    if [ ! -f "$BATCH_SCRIPT" ]; then
        echo "[ERROR] Batch script not found: $BATCH_SCRIPT"
        exit 1
    fi
    
    # Make sure the script is executable
    chmod +x "$BATCH_SCRIPT"
    
    # Check if cron job already exists
    if crontab -l 2>/dev/null | grep -q "$CRON_MARKER"; then
        echo "[INFO] Cron job already exists. Updating..."
        # Remove existing job
        crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | crontab -
    fi
    
    # Add new cron job
    (crontab -l 2>/dev/null; echo "") | crontab -
    (crontab -l 2>/dev/null; echo "$CRON_MARKER") | crontab -
    (crontab -l 2>/dev/null; echo "$CRON_SCHEDULE bash $BATCH_SCRIPT >> $LOG_DIR/cron_execution.log 2>&1") | crontab -
    
    echo ""
    echo "✓ Cron job installed successfully!"
    echo ""
    echo "Schedule: $CRON_SCHEDULE (every 6 hours)"
    echo "Script:   $BATCH_SCRIPT"
    echo "Logs:     $LOG_DIR/cron_execution.log"
    echo ""
    echo "Current crontab:"
    crontab -l | grep -A1 "$CRON_MARKER"
    echo ""
}

# Function to uninstall cron job
uninstall_cron() {
    echo "============================================================"
    echo "  Uninstalling Batch Analytics Cron Job"
    echo "============================================================"
    
    if crontab -l 2>/dev/null | grep -q "$CRON_MARKER"; then
        # Remove the marker and the job line
        crontab -l 2>/dev/null | grep -v "$CRON_MARKER" | grep -v "$BATCH_SCRIPT" | crontab -
        echo "✓ Cron job removed successfully!"
    else
        echo "[INFO] No cron job found to remove."
    fi
    echo ""
}

# Function to show status
show_status() {
    echo "============================================================"
    echo "  Batch Analytics Cron Job Status"
    echo "============================================================"
    echo ""
    
    # Check if cron job is installed
    if crontab -l 2>/dev/null | grep -q "$CRON_MARKER"; then
        echo "Status: INSTALLED ✓"
        echo ""
        echo "Cron Entry:"
        crontab -l | grep -A1 "$CRON_MARKER" | grep -v "^$"
        echo ""
        
        # Show schedule in human-readable format
        echo "Schedule: Every 6 hours (at 00:00, 06:00, 12:00, 18:00)"
        echo ""
        echo "Current Time: $(date '+%Y-%m-%d %H:%M:%S')"
        echo ""
        
        # Show last execution time from log
        if [ -f "$LOG_DIR/cron_execution.log" ]; then
            echo "Last Execution:"
            echo "----------------"
            LAST_RUN=$(grep "Batch Analytics - Starting" "$LOG_DIR/cron_execution.log" | tail -1)
            if [ -n "$LAST_RUN" ]; then
                # Extract timestamp from log file
                LAST_TIME=$(stat -c %y "$LOG_DIR/cron_execution.log" 2>/dev/null | cut -d'.' -f1)
                echo "  Time: $LAST_TIME"
            else
                echo "  No executions logged yet"
            fi
            echo ""
            
            # Show last completion status
            LAST_COMPLETE=$(grep -E "completed successfully|failed with exit code" "$LOG_DIR/cron_execution.log" | tail -1)
            if [ -n "$LAST_COMPLETE" ]; then
                echo "Last Status:"
                echo "  $LAST_COMPLETE"
            fi
        else
            echo "Last Execution: No log file found"
            echo "  (Job may not have run yet)"
        fi
        echo ""
        
        # Show next scheduled run
        echo "Next Scheduled Run:"
        CURRENT_HOUR=$(date +%H)
        CURRENT_MIN=$(date +%M)
        
        # Calculate next run time (at next 6-hour mark: 0, 6, 12, 18)
        # If we're past minute 0 of a scheduled hour, move to the next slot
        CURRENT_SLOT=$((CURRENT_HOUR / 6))
        CURRENT_SLOT_HOUR=$((CURRENT_SLOT * 6))
        
        if [ $CURRENT_HOUR -eq $CURRENT_SLOT_HOUR ] && [ $CURRENT_MIN -gt 0 ]; then
            # We're past the current slot's start time, move to next slot
            NEXT_HOUR=$(((CURRENT_SLOT + 1) * 6))
        else
            # We're before the next slot or exactly at :00
            NEXT_HOUR=$(((CURRENT_SLOT + 1) * 6))
        fi
        
        if [ $NEXT_HOUR -ge 24 ]; then
            NEXT_HOUR=$((NEXT_HOUR - 24))
            NEXT_DAY="tomorrow"
        else
            NEXT_DAY="today"
        fi
        
        echo "  $NEXT_DAY at $(printf "%02d:00" $NEXT_HOUR)"
        echo ""
        
    else
        echo "Status: NOT INSTALLED"
        echo ""
        echo "To install, run: $0 install"
        echo ""
    fi
    
    echo "Log Directory: $LOG_DIR"
    echo ""
}

# Function to show recent logs
show_logs() {
    echo "============================================================"
    echo "  Recent Batch Execution Logs"
    echo "============================================================"
    echo ""
    
    if [ -f "$LOG_DIR/cron_execution.log" ]; then
        echo "Showing last 50 lines of cron execution log:"
        echo "------------------------------------------------------------"
        tail -50 "$LOG_DIR/cron_execution.log"
        echo ""
        echo "Full log: $LOG_DIR/cron_execution.log"
    else
        echo "[INFO] No cron execution log found at:"
        echo "       $LOG_DIR/cron_execution.log"
        echo ""
        echo "The job may not have run yet, or logs are stored elsewhere."
    fi
    echo ""
    
    # Also check main batch logs
    if [ -f "$PROJECT_ROOT/logs/spark_batch_analytics.log" ]; then
        echo ""
        echo "Latest Spark batch analytics log:"
        echo "------------------------------------------------------------"
        tail -30 "$PROJECT_ROOT/logs/spark_batch_analytics.log"
        echo ""
        echo "Full log: $PROJECT_ROOT/logs/spark_batch_analytics.log"
    fi
    echo ""
}

# Main execution
case "${1:-}" in
    install)
        install_cron
        ;;
    uninstall)
        uninstall_cron
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs
        ;;
    *)
        show_usage
        ;;
esac
