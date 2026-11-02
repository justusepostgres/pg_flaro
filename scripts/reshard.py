#!/usr/bin/env python3
"""Resume-safe, single-controller slot moves between configured Flaro shards.

Requires redis-py 6.4.0. One operator at a time; no distributed controller election.
The bootstrap JSON supplies stable endpoints, not current slot ownership.
"""
import argparse
import json
from pathlib import Path

import redis


def command(client, *args):
    # Separate command words bypass redis-py's optional response transformations.
    return client.execute_command(*args)


def owner(client, slot):
    for start, end, primary, *_ in command(client, "CLUSTER", "SLOTS"):
        if start <= slot <= end:
            return primary[2].decode()
    raise RuntimeError(f"slot {slot} has no owner")


def move_slot(nodes, source_id, target_id, slot, timeout=5000):
    if source_id == target_id or not 0 <= slot < 16384:
        raise ValueError("distinct source/target and slot 0..16383 required")
    by_id = {node["id"]: node for node in nodes}
    if len(by_id) != len(nodes):
        raise ValueError("duplicate node IDs")
    source_node, target_node = by_id[source_id], by_id[target_id]
    clients = {node["id"]: redis.Redis(host=node["host"], port=node["port"], socket_timeout=65, protocol=2) for node in nodes}
    try:
        for node_id, client in clients.items():
            if command(client, "CLUSTER", "MYID").decode() != node_id:
                raise RuntimeError(f"endpoint identity mismatch for {node_id}")
            if owner(client, slot) not in (source_id, target_id):
                raise RuntimeError(f"unexpected slot owner at {node_id}; stop concurrent controllers")
        source, target = clients[source_id], clients[target_id]
        pending = command(source, "PGCACHE.MIGRATION")
        if pending is not None:
            if pending[0] != slot or pending[2].decode() != target_id:
                raise RuntimeError("source has another pending transfer; resume that move first")
            command(source, "MIGRATE", target_node["host"], target_node["port"], pending[1], 0, timeout, "REPLACE")
        source_owner, target_owner = owner(source, slot), owner(target, slot)
        if source_owner == target_id and target_owner != target_id:
            raise RuntimeError("source finalized before target; inconsistent external topology change")
        if target_owner == target_id and command(source, "CLUSTER", "COUNTKEYSINSLOT", slot):
            raise RuntimeError("target finalized with keys remaining at source; refusing destructive repair")
        if target_owner != target_id:
            command(target, "CLUSTER", "SETSLOT", slot, "IMPORTING", source_id)
        if source_owner != target_id:
            command(source, "CLUSTER", "SETSLOT", slot, "MIGRATING", target_id)
        moved = 0
        while True:
            # One key bounds replies even when keys approach the frame limit.
            keys = command(source, "CLUSTER", "GETKEYSINSLOT", slot, 1)
            if not keys:
                break
            for key in keys:
                command(source, "MIGRATE", target_node["host"], target_node["port"], key, 0, timeout, "REPLACE")
                moved += 1
        # Target first keeps stale source views safe: ASK reaches the new owner.
        for client in [target, source, *[c for node_id, c in clients.items() if node_id not in (source_id, target_id)]]:
            command(client, "CLUSTER", "SETSLOT", slot, "NODE", target_id)
        for client in clients.values():
            if owner(client, slot) != target_id:
                raise RuntimeError("topology publication incomplete; rerun this command")
        return moved
    finally:
        for client in clients.values():
            client.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True, help="bootstrap JSON containing nodes")
    parser.add_argument("--source", required=True, help="40-character source shard ID")
    parser.add_argument("--target", required=True, help="40-character target shard ID")
    parser.add_argument("--slot", type=int, required=True)
    parser.add_argument("--timeout", type=int, default=5000, help="per-key transfer timeout in ms (1..30000)")
    args = parser.parse_args()
    if not 1 <= args.timeout <= 30000:
        parser.error("timeout must be 1..30000")
    nodes = json.loads(args.config.read_text())["nodes"]
    moved = move_slot(nodes, args.source, args.target, args.slot, args.timeout)
    print(f"Slot {args.slot} now belongs to {args.target}; transferred {moved} keys in this run")


if __name__ == "__main__":
    main()
