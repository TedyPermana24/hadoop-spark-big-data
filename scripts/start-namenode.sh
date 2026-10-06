#!/bin/bash
# Auto-format namenode if not already formatted
NAMEDIR="/opt/hadoop/dfs/name"

if [ ! -d "$NAMEDIR/current" ]; then
    echo "=== NameNode not formatted. Formatting... ==="
    hdfs namenode -format -force -nonInteractive
    echo "=== NameNode formatted successfully ==="
fi

# Execute the original command
exec "$@"
