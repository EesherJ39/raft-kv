package com.example.raftkv;

final class RpcJson {
    private static String esc(String s) {
        return s == null ? "null" : ("\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\"");
    }

    // ----- AppendEntries -----
    static String appendReqToJson(AppendEntries.Request r) {
        StringBuilder sb = new StringBuilder();
        sb.append("{")
          .append("\"term\":").append(r.term).append(',')
          .append("\"leaderId\":").append(esc(r.leaderId)).append(',')
          .append("\"prevLogIndex\":").append(r.prevLogIndex).append(',')
          .append("\"prevLogTerm\":").append(r.prevLogTerm).append(',')
          .append("\"leaderCommit\":").append(r.leaderCommit).append(',')
          .append("\"entries\":[");
        if (r.entries != null) {
            boolean first = true;
            for (AppendEntries.Log e : r.entries) {
                if (!first) sb.append(',');
                first = false;
                sb.append("{\"term\":").append(e.term)
                  .append(",\"key\":").append(esc(e.key))
                  .append(",\"val\":").append(esc(e.val)).append("}");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    static AppendEntries.Response appendRespFromJson(String s) {
        if (s == null) return null;
        AppendEntries.Response r = new AppendEntries.Response();
        r.term = intField(s, "term", 0);
        r.success = boolField(s, "success");
        r.matchIndex = intField(s, "matchIndex", -1);
        return r;
    }

    // ----- RequestVote -----
    static String reqVoteReqToJson(RequestVote.Request r) {
        return "{"
             + "\"term\":" + r.term + ","
             + "\"candidateId\":" + esc(r.candidateId) + ","
             + "\"lastLogIndex\":" + r.lastLogIndex + ","
             + "\"lastLogTerm\":" + r.lastLogTerm
             + "}";
    }

    static RequestVote.Response reqVoteRespFromJson(String s) {
        if (s == null) return null;
        RequestVote.Response r = new RequestVote.Response();
        r.term = intField(s, "term", 0);
        r.voteGranted = boolField(s, "voteGranted");
        return r;
    }

    // ----- tiny parsers -----
    private static int intField(String s, String key, int def) {
        int i = s.indexOf("\"" + key + "\"");
        if (i < 0) return def;
        i = s.indexOf(':', i); if (i < 0) return def;
        int j = i + 1; while (j < s.length() && Character.isWhitespace(s.charAt(j))) j++;
        int k = j; while (k < s.length() && "-0123456789".indexOf(s.charAt(k)) >= 0) k++;
        try { return Integer.parseInt(s.substring(j, k)); } catch (Exception e) { return def; }
    }

    private static boolean boolField(String s, String key) {
        int i = s.indexOf("\"" + key + "\"");
        if (i < 0) return false;
        i = s.indexOf(':', i); if (i < 0) return false;
        int j = i + 1; while (j < s.length() && Character.isWhitespace(s.charAt(j))) j++;
        if (s.regionMatches(true, j, "true", 0, 4))  return true;
        if (s.regionMatches(true, j, "false", 0, 5)) return false;
        return false;
    }
}