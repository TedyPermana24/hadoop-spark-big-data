#!/bin/bash
# Ensure Java symlink matches if updated
ACTIVE_JVM=$(ls -d /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.* 2>/dev/null | head -n 1)
if [ -n "$ACTIVE_JVM" ] && [ ! -d /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.212.b04-0.el7_6.x86_64 ]; then
    ln -sf "$ACTIVE_JVM" /usr/lib/jvm/java-1.8.0-openjdk-1.8.0.212.b04-0.el7_6.x86_64
fi
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk

# Auto-format namenode if not already formatted
NAMEDIR="/opt/hadoop/dfs/name"

if [ ! -d "$NAMEDIR/current" ]; then
    echo "=== NameNode not formatted. Formatting... ==="
    hdfs namenode -format -force -nonInteractive
    echo "=== NameNode formatted successfully ==="
fi

# Execute the original command
exec "$@"

