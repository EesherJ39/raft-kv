package com.example.raftkv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;

/**
 * Replays deterministic failure scenarios: lag, crash/restart, minority-leader
 * writes, leader replacement, duplicate RPC delivery, healing, and convergence.
 */
public final class ChaosHarness {
    private ChaosHarness() {}

    public static void main(String[] args) {
        int scenarios = args.length == 0 ? 1_000 : Integer.parseInt(args[0]);
        long totalWrites = 0;
        long totalBlocked = 0;
        long totalDuplicates = 0;
        long started = System.nanoTime();

        for (int seed = 0; seed < scenarios; seed++) {
            SplittableRandom random = new SplittableRandom(0xC0FFEE + seed);
            List<String> ids = new ArrayList<>(List.of("A", "B", "C"));
            Collections.shuffle(ids, new java.util.Random(random.nextLong()));
            String firstLeader = ids.get(0);
            String laggingFollower = ids.get(1);
            String secondLeader = ids.get(2);
            Map<String, String> expected = new LinkedHashMap<>();

            try (TestCluster cluster = new TestCluster(List.of("A", "B", "C"), 7)) {
                cluster.duplicateEvery(3 + random.nextInt(5));
                require(cluster.elect(firstLeader), "initial election failed", seed);

                int initialWrites = 3 + random.nextInt(8);
                for (int i = 0; i < initialWrites; i++) {
                    String key = "seed-" + seed + "-initial-" + i;
                    String value = "v" + random.nextLong();
                    require(cluster.node(firstLeader).put(key, value).code() == RaftNode.ResultCode.OK,
                            "initial write failed", seed);
                    expected.put(key, value);
                    totalWrites++;
                }

                cluster.crash(laggingFollower);
                int catchupWrites = 4 + random.nextInt(10);
                for (int i = 0; i < catchupWrites; i++) {
                    String key = "seed-" + seed + "-catchup-" + i;
                    String value = "v" + random.nextLong();
                    require(cluster.node(firstLeader).put(key, value).code() == RaftNode.ResultCode.OK,
                            "majority write failed", seed);
                    expected.put(key, value);
                    totalWrites++;
                }
                cluster.restart(laggingFollower);
                cluster.replicate(firstLeader, 3);

                cluster.isolate(firstLeader);
                for (int i = 0; i < 1 + random.nextInt(3); i++) {
                    RaftNode.WriteResult stale = cluster.node(firstLeader).put(
                            "seed-" + seed + "-stale-" + i,
                            "never-commit");
                    require(stale.code() == RaftNode.ResultCode.NO_QUORUM, "minority write committed", seed);
                }

                require(cluster.elect(secondLeader), "replacement election failed", seed);
                int replacementWrites = 3 + random.nextInt(8);
                for (int i = 0; i < replacementWrites; i++) {
                    String key = "seed-" + seed + "-replacement-" + i;
                    String value = "v" + random.nextLong();
                    require(cluster.node(secondLeader).put(key, value).code() == RaftNode.ResultCode.OK,
                            "replacement write failed", seed);
                    expected.put(key, value);
                    totalWrites++;
                }

                cluster.heal();
                cluster.replicate(secondLeader, 5);
                for (String id : cluster.ids()) {
                    require(expected.equals(cluster.values(id)), "state machines diverged at " + id, seed);
                }
                require(cluster.node(firstLeader).status().role() == RaftNode.Role.FOLLOWER,
                        "old leader did not step down", seed);
                totalBlocked += cluster.blockedRpcCount();
                totalDuplicates += cluster.duplicateDeliveryCount();
            }
        }

        double seconds = (System.nanoTime() - started) / 1_000_000_000.0;
        System.out.printf(
                "CHAOS PASS scenarios=%d committedWrites=%d blockedRPCs=%d duplicateDeliveries=%d elapsedSeconds=%.3f%n",
                scenarios,
                totalWrites,
                totalBlocked,
                totalDuplicates,
                seconds);
    }

    private static void require(boolean condition, String message, int seed) {
        if (!condition) throw new AssertionError(message + " (seed=" + seed + ")");
    }
}
